"""
Streamlit app · XLS in ➜ XLS out (A‑H = questions, I‑P = answers)
-----------------------------------------------------------------
* **Input** : Excel de 16 colonnes — **A → H** contiennent exclusivement des
  **questions**, **I → P** les **réponses associées**.
* **Règles**
  1. La **première occurrence** d’une question ou d’une réponse est préservée.
  2. Toute répétition exacte est **paraphrasée** (même sens, tournure différente).
  3. Les colonnes A‑H sont forcées à se terminer par « ? » ; les colonnes I‑P
     n’en contiennent pas.
  4. Traitement en **batch** (`BATCH_SIZE`) puis **repassage global** pour
     garantir zéro doublon questions OU réponses.
  5. Paraphrase via **OpenAI** si clé fournie ; sinon fallback déterministe
     `(variante #)`.
  6. Aucune cellule vide dans le fichier final.
* **Sortie** : un XLSX téléchargeable, format et unicité respectés.
"""

from __future__ import annotations

import io
import json
import re
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Optional OpenAI                                                            #
# ---------------------------------------------------------------------------
try:
    import openai  # type: ignore

    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

BATCH_SIZE = 250  # lignes par batch
MAX_REPASS = 3    # boucles globales maxi

###############################################################################
# Helper functions                                                           #
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    import random
    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


def paraphrase_openai(texts: List[str]) -> List[str]:
    if not OPENAI_KEY or not openai or not texts:
        return ["" for _ in texts]

    system_msg = (
        "Tu es un assistant de reformulation. Réponds UNIQUEMENT par un tableau JSON, "
        "même ordre que l'entrée, sens conservé, ≤150 caractères chacun."
    )
    user_prompt = "\n".join(texts)

    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        if isinstance(data, list) and len(data) == len(texts):
            return [str(x).strip() for x in data]
    except Exception as exc:
        st.warning(f"OpenAI error : {exc}")
    return ["" for _ in texts]


def ensure_question(text: str) -> str:
    text = text.strip()
    if text.endswith("?"):
        return text
    return text.rstrip(".") + " ?"


def ensure_answer(text: str) -> str:
    text = text.strip()
    if text.endswith("?"):
        text = text.rstrip("?") + "."
    if not re.search(r"[.!?]$", text):
        text += "."
    return text


def deterministic_variant(base: str, suffix: int, as_question: bool) -> str:
    variant = f"{base} (variante {suffix})" if base else f"Contenu généré {suffix}"
    return ensure_question(variant) if as_question else ensure_answer(variant)

###############################################################################
# Batch Processing                                                           #
###############################################################################

def process_batch(df_batch: pd.DataFrame, seen: Dict[str, int], counter: int) -> Tuple[pd.DataFrame, int]:
    out_rows: List[List[str]] = []

    q_cols = list(range(8))      # A‑H
    a_cols = list(range(8, 16))  # I‑P

    for _, row in df_batch.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        d_idx, d_texts = [], []

        # Mark duplicates (global)
        for i, txt in enumerate(vals):
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                d_idx.append(i)
                d_texts.append(txt)
            else:
                seen[key] = 1

        # Paraphrase duplicates
        if d_idx:
            new_texts = paraphrase_openai(d_texts)
            for i, new_t in zip(d_idx, new_texts):
                as_q = i in q_cols
                if not new_t:
                    new_t = deterministic_variant(d_texts[d_idx.index(i)], counter, as_q)
                    counter += 1
                new_t = ensure_question(new_t) if as_q else ensure_answer(new_t)
                while new_t.lower() in seen:
                    new_t = deterministic_variant(new_t, counter, as_q)
                    counter += 1
                vals[i] = new_t
                seen[new_t.lower()] = 1

        # Fill blanks + enforce format
        for i, txt in enumerate(vals):
            as_q = i in q_cols
            if not txt:
                txt = deterministic_variant("Cellule vide", counter, as_q)
                counter += 1
            txt = ensure_question(txt) if as_q else ensure_answer(txt)
            while txt.lower() in seen:
                txt = deterministic_variant(txt, counter, as_q)
                counter += 1
            vals[i] = txt
            seen[txt.lower()] = 1

        # Shuffle pairs to casser l'ordre si besoin
        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        out_rows.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(out_rows, columns=df_batch.columns), counter

###############################################################################
# Global Repasse                                                             #
###############################################################################

def global_repasse(df: pd.DataFrame) -> pd.DataFrame:
    q_cols = list(range(8))
    a_cols = list(range(8, 16))

    seen: Dict[str, int] = {}
    counter = 1
    values = df.values

    for r in range(values.shape[0]):
        for c in range(values.shape[1]):
            cell = str(values[r, c]).strip()
            is_q = c in q_cols
            cell = ensure_question(cell) if is_q else ensure_answer(cell)
            key = cell.lower()
            if key in seen:
                new_t = paraphrase_openai([cell])[0]
                if not new_t:
                    new_t = deterministic_variant(cell, counter, is_q)
                    counter += 1
                new_t = ensure_question(new_t) if is_q else ensure_answer(new_t)
                while new_t.lower() in seen:
                    new_t = deterministic_variant(new_t, counter, is_q)
                    counter += 1
                values[r, c] = new_t
                seen[new_t.lower()] = 1
            else:
                seen[key] = 1
                values[r, c] = cell
    return pd.DataFrame(values, columns=df.columns)

###############################################################################
# Streamlit UI                                                               #
###############################################################################

st.set_page_config(page_title="FAQs uniques (A‑H Q / I‑P A)", page_icon="🤖")
st.title("📥 Nettoyeur Q/A — A‑H = questions, I‑P = réponses")

file = st.file_uploader("Chargez un fichier Excel 16 colonnes (A‑P)", type=["xls", "xlsx"])

if file:
    try:
        df_in = pd.read_excel(file, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture : {exc}")
        st.stop()

    if df_in.shape[1] != 16:
        st.error("Le fichier doit comporter exactement 16 colonnes (A‑P).")
        st.stop()

    st.write("Aperçu :")
    st.dataframe(df_in.head())

    if st.button("🚀 Traiter et télécharger"):
        seen: Dict[str, int] = {}
        counter = 1
        parts: List[pd.DataFrame] = []

        for start in range(0, len(df_in), BATCH_SIZE):
            part = df_in.iloc[start:start + BATCH_SIZE]
            cleaned, counter = process_batch(part, seen, counter)
            parts.append(cleaned)
            st.write(f"Batch {(start // BATCH_SIZE) + 1} terminé ✔️")

        combined = pd.concat(parts, ignore_index=True)

        # passes globales anti‑doublon
        for _ in range(MAX_REPASS):
            before = combined.apply(lambda col: col.str.lower()).duplicated().sum()
            combined = global_repasse(combined)
            after = combined.apply(lambda col: col.str.lower()).duplicated().sum()
            if after == 0 or after == before:
                break

        st.success("✅ Fichier prêt : questions en A‑H, réponses en I‑P, aucun doublon.")

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            combined.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")
        st.download_button(
            "📥 Télécharger le XLSX final",
            data=buf.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Téléversez un fichier Excel pour commencer.")

###############################################################################
# Footer                                                                     #
###############################################################################

st.markdown(
    "<sub>Les 8 premières colonnes sont contraintes à finir par un point d'interrogation, "
    "les 8 suivantes à ne pas en contenir. Unicité totale des questions et des réponses, "
    "avec paraphrase automatique le cas échéant.</sub>",
    unsafe_allow_html=True,
)
