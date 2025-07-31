"""
Streamlit app · XLS in ➜ XLS out (A-H = questions, I-P = answers)
------------------------------------------------------------------
* **Input** : fichier Excel 16 colonnes — **A→H** contiennent des **questions**,
  **I→P** les **réponses correspondantes**.
* **Contraintes**
  1. On conserve la **première apparition** d’une question ou d’une réponse.
  2. Tout doublon exact est **paraphrasé** (même sens, autre formulation).
  3. Colonnes A-H → toujours se terminer par « ? » ; colonnes I-P → jamais de « ? ».
  4. Traitement par **lots de 10 lignes** (mémoire maîtrisée) puis **deux
     repasses globales** : ChatGPT re-vérifie et paraphrase encore si besoin.
  5. Paraphrase via **OpenAI** si clé présente ; sinon un fallback ajoute un mot
     clé (« bis », « ter », …) pour garantir l’unicité ― **sans** la mention
     “(variante X)”.
  6. Aucune cellule vide en sortie.
* **Sortie** : fichier XLSX téléchargeable, conforme et sans répétitions.
"""

from __future__ import annotations

import io
import json
import re
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# OpenAI (optionnel)                                                         #
# ---------------------------------------------------------------------------
try:
    import openai  # type: ignore
    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

BATCH_SIZE = 10  # traitement par lots de 10 lignes
MAX_REPASS = 2   # toujours 2 repasses globales

###############################################################################
# Helpers                                                                     #
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    import random
    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


def paraphrase_openai(texts: List[str]) -> List[str]:
    """Paraphrase via OpenAI ou renvoie une liste vide en cas d’indisponibilité."""
    if not OPENAI_KEY or not openai or not texts:
        return ["" for _ in texts]

    system_msg = (
        "Tu es un assistant de reformulation. Réponds UNIQUEMENT par un tableau JSON, "
        "même ordre, sens conservé, max 150 caractères chacun."
    )
    user_msg = "\n".join(texts)

    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        if isinstance(data, list) and len(data) == len(texts):
            return [str(x).strip() for x in data]
    except Exception as e:
        st.warning(f"OpenAI error : {e}")
    return ["" for _ in texts]


def ensure_question(text: str) -> str:
    text = text.strip()
    return text if text.endswith("?") else text.rstrip(". ") + " ?"


def ensure_answer(text: str) -> str:
    text = text.strip()
    if text.endswith("?"):
        text = text.rstrip("?")
    if not re.search(r"[.!?]$", text):
        text += "."
    return text


def deterministic_variant(base: str, idx: int, as_question: bool) -> str:
    """Fallback unique sans le motif (variante X)."""
    markers = [" bis", " ter", " quater", " quinquies", " sexies", " septies", " octies"]
    suffix = markers[idx % len(markers)] if base else f" duplicat {idx}"
    variant = f"{base.rstrip('? .')}{suffix}" if base else suffix.strip()
    return ensure_question(variant) if as_question else ensure_answer(variant)

###############################################################################
# Lot de traitement                                                           #
###############################################################################

def process_batch(df_batch: pd.DataFrame, seen: Dict[str, int], counter: int) -> Tuple[pd.DataFrame, int]:
    rows_out: List[List[str]] = []
    q_cols = list(range(8))
    a_cols = list(range(8, 16))

    for _, row in df_batch.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        dup_idx, dup_texts = [], []

        # marquer les duplicatas globaux
        for i, txt in enumerate(vals):
            if not txt:
                continue
            if txt.lower() in seen:
                dup_idx.append(i)
                dup_texts.append(txt)
            else:
                seen[txt.lower()] = 1

        # paraphrase des duplicatas
        if dup_idx:
            new_texts = paraphrase_openai(dup_texts)
            for i, new_t in zip(dup_idx, new_texts):
                is_q = i in q_cols
                if not new_t:
                    new_t = deterministic_variant(dup_texts[dup_idx.index(i)], counter, is_q)
                    counter += 1
                new_t = ensure_question(new_t) if is_q else ensure_answer(new_t)
                while new_t.lower() in seen:
                    new_t = deterministic_variant(new_t, counter, is_q)
                    counter += 1
                vals[i] = new_t
                seen[new_t.lower()] = 1

        # remplissage des vides + enforcement Q/A
        for i, txt in enumerate(vals):
            is_q = i in q_cols
            if not txt:
                txt = deterministic_variant("Contenu manquant", counter, is_q)
                counter += 1
            txt = ensure_question(txt) if is_q else ensure_answer(txt)
            while txt.lower() in seen:
                txt = deterministic_variant(txt, counter, is_q)
                counter += 1
            vals[i] = txt
            seen[txt.lower()] = 1

        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        rows_out.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(rows_out, columns=df_batch.columns), counter

###############################################################################
# Repassage global (2 tours)                                                  #
###############################################################################

def global_repasse(df: pd.DataFrame) -> pd.DataFrame:
    q_cols = list(range(8))
    a_cols = list(range(8, 16))
    seen: Dict[str, int] = {}
    counter = 1
    arr = df.values

    for r in range(arr.shape[0]):
        for c in range(arr.shape[1]):
            text = str(arr[r, c]).strip()
            is_q = c in q_cols
            text = ensure_question(text) if is_q else ensure_answer(text)
            key = text.lower()
            if key in seen:
                new_t = paraphrase_openai([text])[0]
                if not new_t:
                    new_t = deterministic_variant(text, counter, is_q)
                    counter += 1
                new_t = ensure_question(new_t) if is_q else ensure_answer(new_t)
                while new_t.lower() in seen:
                    new_t = deterministic_variant(new_t, counter, is_q)
                    counter += 1
                arr[r, c] = new_t
                seen[new_t.lower()] = 1
            else:
                seen[key] = 1
                arr[r, c] = text
    return pd.DataFrame(arr, columns=df.columns)

###############################################################################
# Interface Streamlit                                                        #
###############################################################################

st.set_page_config(page_title="FAQs uniques (lots de 10)", page_icon="🤖")
st.title("📥 Nettoyeur Q/A — questions A-H · réponses I-P · lots de 10")

file = st.file_uploader("Téléversez votre Excel (16 colonnes A-P)", type=["xls", "xlsx"])

if file:
    try:
        df_in = pd.read_excel(file, engine="openpyxl")
    except Exception as e:
        st.error(f"Erreur de lecture : {e}")
        st.stop()

    if df_in.shape[1] != 16:
        st.error("Le fichier doit comporter exactement 16 colonnes (A-P).")
        st.stop()

    st.dataframe(df_in.head())

    if st.button("🚀 Traiter et télécharger"):
        seen_global: Dict[str, int] = {}
        counter = 1
        processed: List[pd.DataFrame] = []

        for start in range(0, len(df_in), BATCH_SIZE):
            batch = df_in.iloc[start:start + BATCH_SIZE]
            clean_batch, counter = process_batch(batch, seen_global, counter)
            processed.append(clean_batch)
            st.write(f"Batch {(start // BATCH_SIZE) + 1} terminé ✔️")

        combined = pd.concat(processed, ignore_index=True)

        # deux repasses ChatGPT pour consolidation
        for _ in range(MAX_REPASS):
            combined = global_repasse(combined)

        st.success("✅ Traitement fini : questions uniques A-H, réponses uniques I-P.")

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
    st.info("Chargez un fichier pour commencer.")

###############################################################################
# Footer                                                                     #
###############################################################################

st.markdown(
    "<sub>Deux passes globales assurent l'absence totale de doublons ; les "
    "fallbacks ajoutent ‘bis’, ‘ter’, etc., plutôt que l'ancien suffixe </sub>",
    unsafe_allow_html=True,
)
