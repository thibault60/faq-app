"""
Streamlit app: Local XLS → Cleaned XLS (aucune cellule vide)
------------------------------------------------------------
* Upload an Excel file where columns **A‑H** = Q1…Q8 and **I‑P** = A1…A8.
* **Rule**: every cell in the 16‑column grid must end up **non‑empty**.  We keep
  the first occurrence of a value as‑is; any duplicate (row/col/global) is
  replaced by **new content** so that no blanks remain.
    * If `OPENAI_API_KEY` is provided, the app generates fresh Q/A pairs with
      OpenAI to fill duplicates or holes.
    * Otherwise, it synthesises fallback content guaranteed unique by row/col.
* Pairs are then shuffled per row (Fisher–Yates) to avoid positional bias.
* The final sheet is returned as a downloadable XLSX — no external storage.
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
    """Return *n* brand‑new (Q, A) pairs via OpenAI or blank tuples if disabled."""
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


def fallback_pair(keyword: str, counter: int) -> Tuple[str, str]:
    """Generate a deterministic fallback pair when OpenAI not available."""
    return (
        f"Quelles sont les particularités du {keyword} (variante {counter}) ?",
        f"Cette variante {counter} du {keyword} présente une approche unique répondant aux besoins spécifiques de nos clients.",
    )

###############################################################################
# Core processing
###############################################################################

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create a new DataFrame where duplicates are replaced; no blank cells."""

    if df.shape[1] != 16:
        raise ValueError("Le fichier doit contenir exactement 16 colonnes (A‑P).")

    # Pass 1 : comptage global (casse insensible)
    freq: dict[str, int] = {}
    for v in df.values.flatten(order="C"):
        if pd.isna(v) or not str(v).strip():
            continue
        key = str(v).strip().lower()
        freq[key] = freq.get(key, 0) + 1

    # Pass 2 : traitement ligne par ligne
    seen_once: set[str] = set()
    fallback_counter = 1  # pour générer des contenus uniques sans OpenAI
    cleaned_rows: List[List[str]] = []

    for idx, row in df.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        holes: List[Tuple[int, int]] = []  # paires à remplacer (qi, ai)

        # --- repère duplicatas et marque les trous ------------------------
        for i in range(8):
            qi, ai = i, i + 8
            q, a = vals[qi], vals[ai]

            # Question
            if q:
                kq = q.lower()
                if freq[kq] > 1 and kq in seen_once:  # duplicata non‑premier
                    vals[qi] = ""  # vider pour remplacement
                    holes.append((qi, ai))
                else:
                    seen_once.add(kq)
            else:
                holes.append((qi, ai))

            # Answer
            if a:
                ka = a.lower()
                if freq.get(ka, 0) > 1 and ka in seen_once:
                    vals[ai] = ""
                    if (qi, ai) not in holes:
                        holes.append((qi, ai))
                else:
                    seen_once.add(ka)
            else:
                if (qi, ai) not in holes:
                    holes.append((qi, ai))

        # --- remplissage des trous ---------------------------------------
        if holes:
            keyword = vals[0] or f"élément {idx+1}"
            existing_strings = [v for v in vals if v]
            generated_pairs = generate_openai_pairs(keyword, existing_strings, len(holes))

            for (qi, ai), (q_new, a_new) in zip(holes, generated_pairs):
                # Si OpenAI n'a rien renvoyé, fallback local unique
                if not q_new or not a_new:
                    q_new, a_new = fallback_pair(keyword, fallback_counter)
                    fallback_counter += 1

                # garantir unicité
                while q_new.lower() in seen_once or a_new.lower() in seen_once:
                    q_new, a_new = fallback_pair(keyword, fallback_counter)
                    fallback_counter += 1

                vals[qi], vals[ai] = q_new, a_new
                seen_once.update({q_new.lower(), a_new.lower()})

        # --- shuffle et push ---------------------------------------------
        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        cleaned_rows.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(cleaned_rows, columns=df.columns)

###############################################################################
# Streamlit UI
###############################################################################

st.set_page_config(page_title="FAQs sans doublon (aucune cellule vide)", page_icon="🤖")
st.title("📥 Nettoyeur & compléteur de FAQs — zéro cellule vide")

uploaded = st.file_uploader(
    "Chargez un fichier Excel (.xls/.xlsx) de 16 colonnes (A‑P)",
    type=["xls", "xlsx"],
)

if uploaded:
    try:
        raw_df = pd.read_excel(uploaded, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture : {exc}")
        st.stop()

    st.subheader("Aperçu du fichier importé")
    st.dataframe(raw_df.head())

    if st.button("🚀 Nettoyer, compléter et télécharger"):
        try:
            cleaned_df = process_dataframe(raw_df)
        except Exception as exc:
            st.error(f"Erreur de traitement : {exc}")
            st.stop()

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            cleaned_df.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")
        st.success(f"✅ Traitement terminé : {cleaned_df.shape[0]} lignes, 0 cellule vide.")
        st.download_button(
            label="📥 Télécharger le fichier nettoyé",
            data=buffer.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Importez un classeur pour commencer.")

###############################################################################
# Footer
###############################################################################

st.markdown(
    "<sub>Les cellules uniques restent intactes ; chaque doublon est remplacé "
    "par un contenu original (OpenAI si disponible, sinon fallback local). "
    "Aucune cellule vide dans la sortie.</sub>",
    unsafe_allow_html=True,
)
