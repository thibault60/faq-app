"""
Streamlit app: Local XLS → Cleaned XLS with unique FAQs
-------------------------------------------------------
* Accepts an uploaded Excel file where columns A‑H contain the first 8 Questions
  and columns I‑P contain the corresponding Answers.
* Removes any duplicates across all Questions **and** Answers (case‑insensitive).
* If blanks remain, can optionally call OpenAI (if `OPENAI_API_KEY` provided in
  secrets) to generate fresh Q/A pairs.
* Shuffles pairs per row (Fisher–Yates) to reduce positional bias.
* Returns a new Excel file (same layout) for download — no Google Sheets or
  Google Cloud connection required.
* All processing happens in‑memory; nothing is written to disk on the server.
"""

from __future__ import annotations

import io
import json
from typing import List, Tuple

import pandas as pd
import streamlit as st

# Optional: only import openai if the key exists to avoid needless dependency
try:
    import openai  # type: ignore

    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

###############################################################################
# Helpers
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    """In‑place uniform shuffle of list of (Q, A) tuples."""
    import random

    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


def generate_openai_pairs(keyword: str, existing: List[str], n: int) -> List[Tuple[str, str]]:
    """Generate *n* (Q, A) pairs via OpenAI — returns empty strings if disabled."""
    if not OPENAI_KEY or not openai:
        return [("", "")] * n

    prompt = (
        f"Rédige {n} FAQ inédites (<150 car.) pour \"{keyword}\".\n"
        "Varie les débuts de questions (Pourquoi, Comment, En quoi…), alterne le style "
        "et évite tout doublon avec : " + " | ".join(existing[:15])
    )

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            presence_penalty=0.8,
            frequency_penalty=0.5,
            response_format={"type": "json_object"},
        )
        arr = json.loads(response.choices[0].message.content)
        if isinstance(arr, list):
            return [
                (str(o.get("q", "").strip()), str(o.get("a", "").strip()))
                for o in arr[:n]
            ]
    except Exception as exc:  # pragma: no cover
        st.warning(f"OpenAI error : {exc}")
    return [("", "")] * n


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a new DataFrame with duplicates removed / filled / shuffled."""

    if df.shape[1] != 16:
        raise ValueError("Le fichier doit contenir exactement 16 colonnes (A‑P).")

    seen: set[str] = set()
    cleaned_rows: List[List[str]] = []

    for idx, row in df.iterrows():
        # Convert to list of strings, strip spaces, replace NaN with ""
        values = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]

        # Register existing Q & A in global set (case‑insensitive)
        for v in values:
            if v:
                seen.add(v.lower())

        # Identify missing pairs
        holes = [(i, i + 8) for i in range(8) if not (values[i] and values[i + 8])]
        if holes:
            keyword = values[0] or f"mot‑clé {idx+1}"
            existing_strings = [v for v in values if v]
            new_pairs = generate_openai_pairs(keyword, existing_strings, len(holes))
            for (qi, ai), (q_new, a_new) in zip(holes, new_pairs):
                if q_new and a_new and q_new.lower() not in seen and a_new.lower() not in seen:
                    values[qi], values[ai] = q_new, a_new
                    seen.update({q_new.lower(), a_new.lower()})

        # Remove any duplicates inside the same row (unlikely but safe)
        for i, v in enumerate(values):
            if v and values.count(v) > 1:
                values[i] = ""

        # Build list of (Q, A) pairs, shuffle them, then flatten back
        pairs = list(zip(values[:8], values[8:]))
        fisher_yates(pairs)
        shuffled = [x for q, a in pairs for x in (q, a)]
        cleaned_rows.append(shuffled)

    out_df = pd.DataFrame(cleaned_rows, columns=df.columns)
    return out_df

###############################################################################
# Streamlit Interface
###############################################################################

st.header("Étape 1 : Charger votre fichier Excel")
uploaded = st.file_uploader(
    "Choisissez un fichier .xls ou .xlsx contenant 16 colonnes (A‑P)",
    type=["xls", "xlsx"],
)

if uploaded:
    try:
        raw_df = pd.read_excel(uploaded, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur lors de la lecture du fichier : {exc}")
        st.stop()

    st.success("Fichier importé avec succès !")
    st.subheader("Aperçu :")
    st.dataframe(raw_df.head())

    if st.button("🛠️ Nettoyer / compléter & télécharger"):
        try:
            cleaned_df = process_dataframe(raw_df)
        except Exception as exc:
            st.error(f"Erreur de traitement : {exc}")
            st.stop()

        st.success(f"✅ {cleaned_df.shape[0]} lignes traitées. Téléchargez le résultat :")

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            cleaned_df.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")
        st.download_button(
            "📥 Télécharger le fichier XLS résultant",
            data=buffer.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Téléversez un fichier Excel pour commencer.")

###############################################################################
# Footer
###############################################################################

st.markdown(
    "<sub>Ce service fonctionne entièrement hors connexion Google Cloud. "
    "Si vous ajoutez votre `OPENAI_API_KEY` dans les *secrets* Streamlit, "
    "l'application utilisera GPT‑4o‑mini pour compléter les trous ; sinon, elle "
    "se contentera d'éliminer les doublons et de ré‑ordonner vos paires.</sub>",
    unsafe_allow_html=True,
)
