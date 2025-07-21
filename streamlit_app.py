"""
Streamlit app: FAQ Generator without duplicates
------------------------------------------------
This consolidated version:
* Accepts an Excel upload (A-H questions, I-P answers)
* Enforces global uniqueness across all Q & A values
* Optionally uses OpenAI to fill any blanks (if present)
* Writes the cleaned/augmented data to a Google Sheet tab
  "MODULES FAQs - FINAL" (recreated on each run)
* Secrets (OPENAI_API_KEY, gcp_service_account JSON, sheet_id)
  are read from st.secrets; NEVER commit them.
"""

import io
import json
from typing import List

import openai
import pandas as pd
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# ------------------------- Page config -------------------------------------
st.set_page_config(page_title="Générateur de FAQs", page_icon="🤖")
st.title("📥 Import Excel ➜ FAQs sans doublon → Google Sheets")

# ------------------------- Secrets & clients -------------------------------
openai.api_key = st.secrets.get("OPENAI_API_KEY", "")

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
creds_dict = st.secrets.get("gcp_service_account", {})
if not creds_dict:
    st.warning("Aucun compte de service GCP configuré dans les secrets.")

gs_client = gspread.authorize(
    ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
) if creds_dict else None

# ------------------------- Widgets configuration ---------------------------
sheet_id = st.text_input(
    "ID du Google Sheet *", st.secrets.get("sheet_id", ""), help="copiez l'ID présent dans l'URL de votre feuille Google Sheets"
)

uploaded_file = st.file_uploader(
    "Téléversez un classeur Excel (.xls / .xlsx) – colonnes A→H = Q1…Q8, I→P = A1…A8",
    type=["xls", "xlsx"],
)

run_btn = st.button("🚀 Générer les FAQs & mettre à jour la feuille")

# ------------------------- Utilitaires -------------------------------------

def fisher_yates(arr: List[str]):
    """Shuffle in‑place (uniform)"""
    import random

    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


def generate_openai_pairs(keyword: str, existing: List[str], n: int):
    """Call OpenAI to generate up to *n* new (Q,A) pairs avoiding *existing*"""
    if not openai.api_key:
        return [["", ""]] * n

    prompt = (
        f"Rédige {n} FAQ inédites (<150 car.) pour \"{keyword}\".\n"
        f"Varie les débuts de questions (Pourquoi, Comment, En quoi…), alterne le style et évite tout doublon avec : "
        + " | ".join(existing)
    )

    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        presence_penalty=0.8,
        frequency_penalty=0.5,
        response_format={"type": "json_object"},
    )
    try:
        arr = json.loads(response.choices[0].message.content)
    except Exception:
        return [["", ""]] * n

    if isinstance(arr, list):
        return [
            [o.get("q", ""), o.get("a", "")] for o in arr[:n]
        ]
    return [["", ""]] * n


def process_dataframe(df: pd.DataFrame, max_pairs: int = 8):
    """Return cleaned data list‑of‑lists [header, *rows*] ensuring global uniqueness"""
    if df.shape[1] != 16:
        raise ValueError("Le fichier doit contenir exactement 16 colonnes (A‑P).")

    header = list(df.columns)
    data_out = [header]
    seen = set()

    for idx, row in df.iterrows():
        values = [str(v).strip() if not pd.isna(v) else "" for v in row.tolist()]

        # mark existing values
        for v in values:
            if v:
                seen.add(v.lower())

        # detect holes (empty questions or answers)
        holes = [i for i, v in enumerate(values) if v == ""]

        if holes:
            keyword = values[0] or f"mot‑clé {idx+1}"
            existing_q = [values[i] for i in range(16) if values[i]]
            pairs_needed = len(holes) // 2  # each pair = 2 holes (Q & A)
            new_pairs = generate_openai_pairs(keyword, existing_q, pairs_needed)
            pair_idx = 0
            for i in holes:
                if i < 8:  # question col
                    q, a = new_pairs[pair_idx]
                    if q and q.lower() not in seen and a and a.lower() not in seen:
                        values[i] = q
                        values[i + 8] = a
                        seen.add(q.lower())
                        seen.add(a.lower())
                    pair_idx += 1

        # now enforce uniqueness per cell
        for i in range(16):
            v = values[i]
            if v and list(values).count(v) > 1:
                values[i] = ""  # clear duplicate inside the row

        # shuffle pairs to avoid order bias
        q_cols = values[:8]
        a_cols = values[8:]
        pairs = list(zip(q_cols, a_cols))
        fisher_yates(pairs)
        values = [x for p in pairs for x in p]

        data_out.append(values)

    return data_out


# ------------------------- Action handler ----------------------------------
if run_btn and uploaded_file:
    try:
        df = pd.read_excel(uploaded_file, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture Excel : {exc}")
        st.stop()

    try:
        output = process_dataframe(df)
    except Exception as exc:
        st.error(f"Erreur de traitement : {exc}")
        st.stop()

    if not gs_client:
        st.error("Client Google Sheets non initialisé – secrets manquants ?")
        st.stop()

    try:
        sh = gs_client.open_by_key(sheet_id)
    except Exception as exc:
        st.error(f"Impossible d'ouvrir le classeur : {exc}")
        st.stop()

    # recreate destination sheet
    try:
        dest_ws = sh.worksheet("MODULES FAQs - FINAL")
        sh.del_worksheet(dest_ws)
    except Exception:
        pass
    dest_ws = sh.add_worksheet("MODULES FAQs - FINAL", rows=len(output), cols=16)

    dest_ws.update("A1", output)
    st.success(f"✅ {len(output)-1} lignes écrites dans MODULES FAQs - FINAL.")
    st.balloons()
else:
    st.info("Veuillez sélectionner un fichier Excel puis cliquer sur le bouton.")
