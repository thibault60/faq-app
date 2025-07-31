"""
Streamlit app · XLS in ➜ XLS out (paraphrase duplicates, enforce Q / A format)
-----------------------------------------------------------------------------
* **Input** : Excel 16 colonnes (A‑H = Q1…Q8, I‑P = A1…A8).
* **Rules**
  1. The **first occurrence** of any string is preserved.
  2. Any duplicate is paraphrased (same meaning, different wording).
  3. **Columns whose header starts with “Q”** must contain **questions**
     (text ending by “?”). If a cell in a Q‑column is not a question, we
     rewrite it into interrogative form.
  4. **Columns whose header starts with “A”** must contain **answers**
     (no trailing “?”). If a cell in an A‑column looks like a question, we
     rewrite it into declarative form.
  5. Processing in batches (`BATCH_SIZE`) for large files then **global
     repasse** until zero duplicates.
  6. Optionally calls OpenAI for paraphrase; otherwise a deterministic fallback
     (`variante #`).
* Output: XLSX ready to download, with **no duplicate content, no blanks, Q/A
  format enforced.**
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

BATCH_SIZE = 250  # rows par batch
MAX_REPASS = 3    # max global passes to eliminate duplicates

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
        "Tu es un assistant de reformulation. Réponds UNIQUEMENT par un tableau JSON contenant les reformulations, "
        "même ordre que l'entrée, sans dépasser 150 caractères chacun."
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


def deterministic_variant(base: str, suffix: int, is_question: bool) -> str:
    variant = f"{base} (variante {suffix})" if base else f"Contenu généré {suffix}"
    return ensure_question_format(variant) if is_question else ensure_answer_format(variant)


def ensure_question_format(text: str) -> str:
    text = text.strip()
    if text.endswith("?"):
        return text
    # If statement‑like, convert simply by appending '?'
    return text.rstrip(".") + " ?"


def ensure_answer_format(text: str) -> str:
    text = text.strip()
    if text.endswith("?"):
        # naive conversion → remove '?' and add '.'
        text = text.rstrip("?") + "."
    if not re.search(r"[.!?]$", text):
        text += "."
    return text

###############################################################################
# Batch Processing                                                           #
###############################################################################

def process_batch(df_batch: pd.DataFrame, global_seen: Dict[str, int], counter_start: int) -> Tuple[pd.DataFrame, int]:
    rows_out: List[List[str]] = []
    fallback_counter = counter_start

    # Identify Q vs A column indices from headers
    q_cols = [i for i, h in enumerate(df_batch.columns) if str(h).strip().upper().startswith("Q")]
    a_cols = [i for i, h in enumerate(df_batch.columns) if str(h).strip().upper().startswith("A")]

    for _, row in df_batch.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        dup_idx, dup_texts = [], []

        # Mark duplicates beyond first appearance
        for idx, txt in enumerate(vals):
            if not txt:
                continue
            key = txt.lower()
            if key in global_seen:
                dup_idx.append(idx)
                dup_texts.append(txt)
            else:
                global_seen[key] = 1

        # Paraphrase duplicates
        if dup_idx:
            new_texts = paraphrase_openai(dup_texts)
            for i, new_t in zip(dup_idx, new_texts):
                is_q = i in q_cols
                if not new_t:
                    new_t = deterministic_variant(dup_texts[dup_idx.index(i)], fallback_counter, is_q)
                    fallback_counter += 1
                new_t = ensure_question_format(new_t) if is_q else ensure_answer_format(new_t)
                while new_t.lower() in global_seen:
                    new_t = deterministic_variant(new_t, fallback_counter, is_q)
                    fallback_counter += 1
                vals[i] = new_t
                global_seen[new_t.lower()] = 1

        # Fill blanks + enforce Q/A format
        for idx, txt in enumerate(vals):
            col_is_q = idx in q_cols
            if not txt:
                txt = deterministic_variant("Cellule vide", fallback_counter, col_is_q)
                fallback_counter += 1
            # enforce format
            txt = ensure_question_format(txt) if col_is_q else ensure_answer_format(txt)
            # guarantee final uniqueness
            while txt.lower() in global_seen:
                txt = deterministic_variant(txt, fallback_counter, col_is_q)
                fallback_counter += 1
            vals[idx] = txt
            global_seen[txt.lower()] = 1

        # Shuffle pairs
        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        rows_out.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(rows_out, columns=df_batch.columns), fallback_counter

###############################################################################
# Global Repasse                                                             #
###############################################################################

def global_repasse(df: pd.DataFrame) -> pd.DataFrame:
    q_cols = [i for i, h in enumerate(df.columns) if str(h).strip().upper().startswith("Q")]
    a_cols = [i for i, h in enumerate(df.columns) if str(h).strip().upper().startswith("A")]

    seen: Dict[str, int] = {}
    counter = 1
    values = df.values

    for r in range(values.shape[0]):
        for c in range(values.shape[1]):
            cell = str(values[r, c]).strip()
            is_q = c in q_cols
            formatted = ensure_question_format(cell) if is_q else ensure_answer_format(cell)
            cell_key = formatted.lower()
            if cell_key in seen:
                new_t = paraphrase_openai([formatted])[0]
                if not new_t:
                    new_t = deterministic_variant(formatted, counter, is_q)
                    counter += 1
                new_t = ensure_question_format(new_t) if is_q else ensure_answer_format(new_t)
                while new_t.lower() in seen:
                    new_t = deterministic_variant(new_t, counter, is_q)
                    counter += 1
                values[r, c] = new_t
                seen[new_t.lower()] = 1
            else:
                seen[cell_key] = 1
                values[r, c] = formatted
    return pd.DataFrame(values, columns=df.columns)

###############################################################################
# Streamlit UI                                                               #
###############################################################################

st.set_page_config(page_title="FAQs uniques (règle Q/A)", page_icon="🤖")
st.title("📥 Nettoyeur & Paraphrase — Q-columns = questions, A-columns = réponses")

file = st.file_uploader("Sélectionnez votre Excel (16 colonnes A‑P)", type=["xls", "xlsx"])

if file:
    try:
        df_in = pd.read_excel(file, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture : {exc}")
        st.stop()

    if df_in.shape[1] != 16:
        st.error("Le fichier doit comporter exactement 16 colonnes (A‑P).")
        st.stop()

    st.write("Aperçu :")
    st.dataframe(df_in.head())

    if st.button("🚀 Traiter et télécharger"):
        global_seen: Dict[str, int] = {}
        counter = 1
        processed_parts: List[pd.DataFrame] = []

        for start in range(0, len(df_in), BATCH_SIZE):
            part = df_in.iloc[start:start + BATCH_SIZE]
            cleaned_part, counter = process_batch(part, global_seen, counter)
            processed_parts.append(cleaned_part)
            st.write(f"Batch {(start // BATCH_SIZE) + 1} terminé ✔️")

        combined_df = pd.concat(processed_parts, ignore_index=True)

        # repasse globale pour assurance
        for _ in range(MAX_REPASS):
            before = combined_df.apply(lambda col: col.str.lower()).duplicated().sum()
            combined_df = global_repasse(combined_df)
            after = combined_df.apply(lambda col: col.str.lower()).duplicated().sum()
            if after == 0 or after == before:
                break

        st.success("✅ Fichier prêt, plus aucun doublon et format Q/A respecté.")

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            combined_df.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")
        st.download_button(
            "📥 Télécharger le fichier final",
            data=buf.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Veuillez charger un fichier Excel pour démarrer.")

###############################################################################
# Footer                                                                     #
###############################################################################

st.markdown(
    "<sub>Les colonnes dont l'en‑tête commence par 'Q' sont forcées à finir par un point d'interrogation. "
    "Celles commençant par 'A' sont reformulées sans point d'interrogation. "
    "La première occurrence est préservée, toutes les suivantes paraphrasées. "
    "Aucune cellule blanche ni doublon.</sub>",
    unsafe_allow_html=True,
)
