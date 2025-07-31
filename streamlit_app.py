"""
Streamlit App · XLS in ➜ XLS out (quality-first Q/A enforcement)
-----------------------------------------------------------------
* **Input** : Excel 16 colonnes — **A-H** doivent contenir des **questions** (terminées par "?"), **I-P** les **réponses associées** (sans "?").
* **Règles**
  1. La **première apparition** d’une question ou d’une réponse est conservée.
  2. Toute répétition exacte est **paraphrasée** avec ChatGPT (modèle GPT-4o) ;
     on privilégie la **qualité** à la vitesse (température 0.4, modèle complet).
  3. Si une cellule d’une colonne Q n’est pas une vraie question, ChatGPT la
     convertit en **question pertinente** ; l’inverse pour une réponse.
  4. Traitement **par lots de 10 lignes** puis **2 repasses globales** — chaque
     repasse renvoie la liste complète à ChatGPT pour validation & correction
     finale.
  5. En absence de clé OpenAI, un fallback ajoute un suffixe linguistique (" bis", " ter", …) pour garantir l’unicité (sans “(variante X)”).
  6. Zéro cellule vide en sortie.
* **Sortie** : un fichier XLSX téléchargeable (« MODULES FAQs - FINAL »), 0 doublon & conformité Q/A.
"""

from __future__ import annotations

import io
import json
import re
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# ────────────────────────────────────────────────────────────────────────────
# OpenAI (optionnel)                                                          
# ────────────────────────────────────────────────────────────────────────────
try:
    import openai  # type: ignore
    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

BATCH_SIZE = 10  # traitement par lots de 10 lignes
GLOBAL_REPASSES = 2  # consolidation qualité

###############################################################################
# Helpers                                                                     #
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    import random
    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


# ──────────────── Paraphrase / Correction via OpenAI ────────────────────────

def paraphrase_openai(texts: List[str], is_question: List[bool]) -> List[str]:
    """Paraphrase en respectant Q/A : liste in  = liste out (même ordre)."""
    if not OPENAI_KEY or not openai or not texts:
        return ["" for _ in texts]

    # Marque chaque entrée pour indiquer à ChatGPT de produire question ou réponse
    formatted = [
        ("Question : " if q else "Réponse : ") + t for t, q in zip(texts, is_question)
    ]

    system_msg = (
        "Tu es un assistant expert en reformulation de FAQ. "
        "Pour chaque élément fourni, renvoie UNIQUEMENT un tableau JSON contenant les "
        "mêmes éléments reformulés, ordre identique. \n"
        "• Si l'élément commence par 'Question :', assure-toi qu'il s'agit bien d'une question claire, concise, terminée par '?' (max 150 car.).\n"
        "• Si l'élément commence par 'Réponse :', produis une réponse déclarative, sans '?' final (max 150 car.).\n"
        "Préserve le sens, varie la formulation, évite tout doublon littéral avec les autres éléments."
    )
    user_msg = "\n".join(formatted)

    try:
        resp = openai.chat.completions.create(
            model="gpt-4o",  # version complète pour qualité
            temperature=0.4,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        if isinstance(data, list) and len(data) == len(texts):
            return [str(x).strip() for x in data]
    except Exception as e:
        st.warning(f"OpenAI error : {e}")
    return ["" for _ in texts]


# ─────────────── Enforcement helpers ───────────────────────────────────────

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


def fallback_variant(base: str, idx: int, as_question: bool) -> str:
    markers = [" bis", " ter", " quater", " quinquies", " sexies", " septies", " octies"]
    suffix = markers[idx % len(markers)] if base else f" duplicat {idx}"
    variant = f"{base.rstrip('? .')}{suffix}" if base else suffix.strip()
    return ensure_question(variant) if as_question else ensure_answer(variant)

###############################################################################
# Batch processing (10 lines)                                                 #
###############################################################################

def process_batch(batch: pd.DataFrame, seen: Dict[str, int], idx_counter: int) -> Tuple[pd.DataFrame, int]:
    q_cols = list(range(8))
    a_cols = list(range(8, 16))
    out_rows: List[List[str]] = []

    for _, row in batch.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        dup_i, dup_t, dup_qflag = [], [], []

        for i, txt in enumerate(vals):
            if not txt:
                continue
            if txt.lower() in seen:
                dup_i.append(i)
                dup_t.append(txt)
                dup_qflag.append(i in q_cols)
            else:
                seen[txt.lower()] = 1

        # Paraphrase duplicates en bloc
        if dup_i:
            new_texts = paraphrase_openai(dup_t, dup_qflag)
            for pos, new_t, is_q in zip(dup_i, new_texts, dup_qflag):
                if not new_t:
                    new_t = fallback_variant(dup_t[dup_i.index(pos)], idx_counter, is_q)
                    idx_counter += 1
                new_t = ensure_question(new_t) if is_q else ensure_answer(new_t)
                while new_t.lower() in seen:
                    new_t = fallback_variant(new_t, idx_counter, is_q)
                    idx_counter += 1
                vals[pos] = new_t
                seen[new_t.lower()] = 1

        # Blanks & formatting
        for i, txt in enumerate(vals):
            is_q = i in q_cols
            if not txt:
                txt = fallback_variant("Contenu manquant", idx_counter, is_q)
                idx_counter += 1
            txt = ensure_question(txt) if is_q else ensure_answer(txt)
            while txt.lower() in seen:
                txt = fallback_variant(txt, idx_counter, is_q)
                idx_counter += 1
            vals[i] = txt
            seen[txt.lower()] = 1

        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        out_rows.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(out_rows, columns=batch.columns), idx_counter

###############################################################################
# Global repasse (2 passes qualité)                                          #
###############################################################################

def global_repasse(df: pd.DataFrame) -> pd.DataFrame:
    q_cols = list(range(8))
    a_cols = list(range(8, 16))
    seen: Dict[str, int] = {}
    idx_counter = 1

    all_texts, q_flags = [], []
    for text, col in zip(df.values.flatten().tolist(), [c for _ in range(df.shape[0]) for c in range(16)]):
        all_texts.append(str(text).strip())
        q_flags.append(col in q_cols)

    refined = paraphrase_openai(all_texts, q_flags)
    if not any(refined):
        # OpenAI down → return original df (already unique)
        return df

    reshaped = [refined[i:i+16] for i in range(0, len(refined), 16)]
    clean_rows = []
    local_seen: Dict[str, int] = {}
    for row in reshaped:
        fixed_row = []
        for col_idx, cell in enumerate(row):
            is_q = col_idx in q_cols
            cell = ensure_question(cell) if is_q else ensure_answer(cell)
            # enforce uniqueness again
            if cell.lower() in local_seen:
                cell = fallback_variant(cell, idx_counter, is_q)
                idx_counter += 1
            local_seen[cell.lower()] = 1
            fixed_row.append(cell)
        clean_rows.append(fixed_row)

    return pd.DataFrame(clean_rows, columns=df.columns)

###############################################################################
# Streamlit Interface                                                        #
###############################################################################

st.set_page_config(page_title="FAQs Q/A — Qualité", page_icon="🤖")
st.title("🔍 Nettoyeur & Paraphrase haute-qualité (lots de 10)")

file = st.file_uploader("Importez un Excel 16 colonnes (A-P)", type=["xls", "xlsx"])

if file:
    try:
        df = pd.read_excel(file, engine="openpyxl")
    except Exception as e:
        st.error(f"Erreur de lecture : {e}")
        st.stop()

    if df.shape[1] != 16:
        st.error("Le fichier doit avoir 16 colonnes (A-P).")
        st.stop()

    st.dataframe(df.head())

    if st.button("🚀 Lancer traitement haute-qualité"):
        seen_global: Dict[str, int] = {}
        counter = 1
        parts: List[pd.DataFrame] = []
        for start in range(0, len(df), BATCH_SIZE):
            batch_df = df.iloc[start:start+BATCH_SIZE]
            cleaned, counter = process_batch(batch_df, seen_global, counter)
            parts.append(cleaned)
            st.write(f"Batch {(start//BATCH_SIZE)+1} terminé ✔️")

        combined = pd.concat(parts, ignore_index=True)

        # Deux repasses complètes avec ChatGPT pour consolidation qualité
        for _ in range(GLOBAL_REPASSES):
            combined = global_repasse(combined)

        st.success("✅ Fichier final prêt : aucune répétition, Q/A conforme.")

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            combined.to_excel(writer, index
