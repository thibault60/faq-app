"""
Streamlit app: Local XLS → Cleaned XLS with unique FAQs
-------------------------------------------------------
* Upload an Excel file where columns **A‑H** = Q1…Q8 and **I‑P** = A1…A8.
* **Rule requested**: **only duplicate cells are modified** (across the whole
  sheet, any column/row).  Cells whose content occurs exactly once remain
  strictly untouched.
    * The **first encounter** of a value is kept; subsequent occurrences are
      considered duplicates.
* For every duplicate cleared (creating a hole) a fresh Q/A pair can be
  generated via OpenAI (if `OPENAI_API_KEY` is provided); otherwise the cell
  stays blank.
* All returned data are unique.  Pairs are shuffled per row (Fisher–Yates) to
  avoid positional bias.
* The processed sheet is offered as a downloadable XLSX — no external storage.
"""

from __future__ import annotations

import io
import json
from typing import List, Tuple

import pandas as pd
import streamlit as st

# Optional OpenAI support ----------------------------------------------------
try:
    import openai  # type: ignore

    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

###############################################################################
# Helper functions
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    """Uniform in‑place shuffle of list of (Q, A) tuples."""
    import random

    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


def generate_openai_pairs(keyword: str, existing: List[str], n: int) -> List[Tuple[str, str]]:
    """Return *n* brand‑new (Q, A) pairs or blank tuples if OpenAI disabled."""
    if not OPENAI_KEY or not openai or n == 0:
        return [("", "")] * n

    prompt = (
        f"Rédige {n} FAQ inédites (<150 car.) pour \"{keyword}\".\n"
        "Varie les débuts de questions (Pourquoi, Comment, En quoi…), alterne le style "
        "et évite tout doublon avec : " + " | ".join(existing[:20])
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
        data = json.loads(response.choices[0].message.content)
        if isinstance(data, list):
            return [
                (str(o.get("q", "").strip()), str(o.get("a", "").strip()))
                for o in data[:n]
            ]
    except Exception as exc:  # pragma: no cover
        st.warning(f"OpenAI error : {exc}")
    return [("", "")] * n

###############################################################################
# Core processing
###############################################################################

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create a new DataFrame where only duplicate cells are changed / filled."""

    if df.shape[1] != 16:
        raise ValueError("Le fichier doit contenir exactement 16 colonnes (A‑P).")

    # --- 1) First pass: count frequencies (case‑insensitive) ----------------
    freq: dict[str, int] = {}
    for val in df.values.flatten(order="C"):
        if pd.isna(val) or not str(val).strip():
            continue
        key = str(val).strip().lower()
        freq[key] = freq.get(key, 0) + 1

    # --- 2) Second pass: build clean DataFrame -----------------------------
    seen_once: set[str] = set()  # records first kept occurrence
    cleaned_rows: List[List[str]] = []

    for idx, row in df.iterrows():
        values = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        holes: List[Tuple[int, int]] = []  # list of (question_idx, answer_idx)

        # Walk through 16 columns (Q1..Q8 + A1..A8)
        for i in range(8):
            qi, ai = i, i + 8
            q, a = values[qi], values[ai]

            # --- Handle Question ------------------------------------------
            if q:
                kq = q.lower()
                if freq[kq] > 1:  # duplicate somewhere
                    if kq in seen_once:  # not the first occurrence
                        values[qi] = ""  # clear, will be filled later
                        holes.append((qi, ai))
                    else:
                        seen_once.add(kq)  # keep first occurrence untouched
            else:
                holes.append((qi, ai))  # missing Q automatically a hole

            # --- Handle Answer -------------------------------------------
            if a:
                ka = a.lower()
                if freq.get(ka, 0) > 1:
                    if ka in seen_once:
                        values[ai] = ""
                        # ensure hole captured (if not already)
                        if (qi, ai) not in holes:
                            holes.append((qi, ai))
                    else:
                        seen_once.add(ka)
            else:
                if (qi, ai) not in holes:
                    holes.append((qi, ai))

        # --- Fill holes via OpenAI ----------------------------------------
        if holes:
            keyword = values[0] or f"mot‑clé {idx+1}"
            existing_strings = [v for v in values if v]
            new_pairs = generate_openai_pairs(keyword, existing_strings, len(holes))

            for (qi, ai), (q_new, a_new) in zip(holes, new_pairs):
                if q_new and a_new and q_new.lower() not in seen_once and a_new.lower() not in seen_once:
                    values[qi], values[ai] = q_new, a_new
                    seen_once.update({q_new.lower(), a_new.lower()})
                # If OpenAI disabled or duplicate found, leave cells blank.

        # --- Shuffle pairs per row ----------------------------------------
        pairs = list(zip(values[:8], values[8:]))
        fisher_yates(pairs)
        shuffled = [x for q, a in pairs for x in (q, a)]
        cleaned_rows.append(shuffled)

    return pd.DataFrame(cleaned_rows, columns=df.columns)

###############################################################################
# Streamlit UI
###############################################################################

st.set_page_config(page_title="Générateur FAQs anti‑doublons", page_icon="🤖")
st.title("📥 Nettoyeur de FAQs — ne modifie que les doublons")

uploaded = st.file_uploader(
    "Téléversez un Excel .xls/.xlsx (16 colonnes : A‑P)",
    type=["xls", "xlsx"],
)

if uploaded:
    try:
        raw_df = pd.read_excel(uploaded, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture : {exc}")
        st.stop()

    st.success("Fichier chargé. Voici un aperçu :")
    st.dataframe(raw_df.head())

    if st.button("🛠️ Nettoyer les doublons et télécharger"):
        try:
            cleaned_df = process_dataframe(raw_df)
        except Exception as exc:
            st.error(f"Erreur de traitement : {exc}")
            st.stop()

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            cleaned_df.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")
        st.success(f"✅ Traitement terminé ({cleaned_df.shape[0]} lignes). Téléchargez ci‑dessous :")
        st.download_button(
            label="📥 Télécharger le fichier nettoyé",
            data=buffer.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Importez un fichier pour commencer.")

###############################################################################
# Footer
###############################################################################

st.markdown(
    "<sub>Les cellules uniques sont préservées à l'identique ; seules les "
    "occurrences répétées sont supprimées et, si possible, remplacées par de "
    "nouvelles FAQs générées (option OpenAI).</sub>",
    unsafe_allow_html=True,
)
