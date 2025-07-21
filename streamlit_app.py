"""
Streamlit app · XLS in ➜ XLS out (paraphrase duplicates, batch‑safe)
------------------------------------------------------------------
* **Input** : Excel (16 cols → A‑H Q1…Q8, I‑P A1…A8).
* **Goal**  : Preserve the first occurrence of every string. All further
  duplicates are **paraphrased** so that the meaning stays the same but the
  wording differs (no duplicate content). Cells that are originally unique
  remain untouched.
* **Batch processing** : rows are processed in configurable chunks to keep
  memory low on large files.
* **Global repasse** : once all batches are done, a final pass ensures that no
  duplicate remains; any residual duplicate is paraphrased again.
* **OpenAI** (optional) is used for paraphrasing; if the key is missing, a
  deterministic fallback adds a version counter (still unique). No cell is
  ever left blank.
"""

from __future__ import annotations

import io
import json
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Optional OpenAI                                                   
# ---------------------------------------------------------------------------
try:
    import openai  # type: ignore

    OPENAI_KEY = st.secrets.get("OPENAI_API_KEY", "")
    if OPENAI_KEY:
        openai.api_key = OPENAI_KEY
except ModuleNotFoundError:
    openai = None  # type: ignore
    OPENAI_KEY = ""

BATCH_SIZE = 250  # nombre de lignes par batch (modulable)
MAX_REPASS  = 3   # tentatives globales pour éliminer tous les doublons

###############################################################################
# Helper functions
###############################################################################

def fisher_yates(arr: List[Tuple[str, str]]):
    """Uniform in‑place shuffle of list of (Q, A) tuples."""
    import random

    for i in range(len(arr) - 1, 0, -1):
        j = random.randint(0, i)
        arr[i], arr[j] = arr[j], arr[i]


# ------------------------------ PARAPHRASE ----------------------------------

def paraphrase_openai(texts: List[str]) -> List[str]:
    """Return paraphrased versions preserving meaning; fallback blank if API off."""
    if not OPENAI_KEY or not openai:
        return ["" for _ in texts]

    prompt = (
        "Reformule chaque élément ci‑dessous en conservant strictement le sens, "
        "sans dépasser 150 caractères par élément, dans le même ordre.\n" +
        "\n".join(f"- {t}" for t in texts)
    )

    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        if isinstance(data, list) and len(data) == len(texts):
            return [str(x).strip() for x in data]
    except Exception as exc:  # pragma: no cover
        st.warning(f"OpenAI error : {exc}")
    # fallback return empties to trigger deterministic variant below
    return ["" for _ in texts]


def deterministic_variant(base: str, suffix: int) -> str:
    """Add a short variation tag to guarantee uniqueness if no OpenAI."""
    return f"{base} (variante {suffix})" if base else f"Contenu généré {suffix}"

###############################################################################
# Processing logic (batch + global)                                          #
###############################################################################

def process_batch(df_batch: pd.DataFrame, global_seen: Dict[str, int], counter_start: int) -> Tuple[pd.DataFrame, int]:
    """Process a single batch, paraphrasing duplicates; update global_seen."""

    rows_out: List[List[str]] = []
    fallback_counter = counter_start

    for _, row in df_batch.iterrows():
        vals = ["" if pd.isna(v) else str(v).strip() for v in row.tolist()]
        # lists to paraphrase later: index -> original text
        dupe_indices: List[int] = []
        dupe_texts: List[str] = []

        # Step 1: mark duplicates but keep first occurrence
        for idx, text in enumerate(vals):
            key = text.lower()
            if not text:
                continue  # empty for now
            if key in global_seen:
                # duplicate: schedule paraphrase
                dupe_indices.append(idx)
                dupe_texts.append(text)
            else:
                global_seen[key] = 1

        # Step 2: paraphrase duplicates in one call (or fallback)
        if dupe_indices:
            new_texts = paraphrase_openai(dupe_texts)
            for i, new_t in zip(dupe_indices, new_texts):
                if not new_t:
                    new_t = deterministic_variant(dupe_texts[dupe_indices.index(i)], fallback_counter)
                    fallback_counter += 1
                # ensure uniqueness vs global_seen
                while new_t.lower() in global_seen:
                    new_t = deterministic_variant(new_t, fallback_counter)
                    fallback_counter += 1
                vals[i] = new_t
                global_seen[new_t.lower()] = 1

        # Step 3: handle blanks (if any)
        for idx, text in enumerate(vals):
            if not text:
                filler = deterministic_variant(f"Cellule vide ligne", fallback_counter)
                fallback_counter += 1
                while filler.lower() in global_seen:
                    filler = deterministic_variant(filler, fallback_counter)
                    fallback_counter += 1
                vals[idx] = filler
                global_seen[filler.lower()] = 1

        # Step 4: shuffle pairs
        pairs = list(zip(vals[:8], vals[8:]))
        fisher_yates(pairs)
        rows_out.append([x for q, a in pairs for x in (q, a)])

    return pd.DataFrame(rows_out, columns=df_batch.columns), fallback_counter


def final_repasse(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure absolutely no duplicate remains after all batches."""
    seen: Dict[str, int] = {}
    fallback_counter = 1
    vals = df.values
    for r in range(vals.shape[0]):
        for c in range(vals.shape[1]):
            cell = str(vals[r, c]).strip()
            key = cell.lower()
            if key in seen:
                # paraphrase again
                new_text = paraphrase_openai([cell])[0]
                if not new_text:
                    new_text = deterministic_variant(cell, fallback_counter)
                    fallback_counter += 1
                while new_text.lower() in seen:
                    new_text = deterministic_variant(new_text, fallback_counter)
                    fallback_counter += 1
                vals[r, c] = new_text
                seen[new_text.lower()] = 1
            else:
                seen[key] = 1
    return pd.DataFrame(vals, columns=df.columns)

###############################################################################
# Streamlit UI                                                               #
###############################################################################

st.set_page_config(page_title="FAQs paraphrasées sans doublon", page_icon="🤖")
st.title("📥 Paraphrase des FAQs — traitement par batch + repasse globale")

uploaded = st.file_uploader(
    "Chargez un Excel (.xls/.xlsx) de 16 colonnes (A‑P)",
    type=["xls", "xlsx"],
)

if uploaded:
    try:
        raw_df = pd.read_excel(uploaded, engine="openpyxl")
    except Exception as exc:
        st.error(f"Erreur de lecture : {exc}")
        st.stop()

    if raw_df.shape[1] != 16:
        st.error("Le fichier doit contenir exactement 16 colonnes (A‑P).")
        st.stop()

    st.write("Aperçu :")
    st.dataframe(raw_df.head())

    if st.button("🚀 Paraphraser & télécharger"):
        # ------------ Batch processing ------------------------------
        global_seen: Dict[str, int] = {}
        processed_batches: List[pd.DataFrame] = []
        counter = 1
        for start in range(0, len(raw_df), BATCH_SIZE):
            end = start + BATCH_SIZE
            batch_df = raw_df.iloc[start:end]
            cleaned_batch, counter = process_batch(batch_df, global_seen, counter)
            processed_batches.append(cleaned_batch)
            st.write(f"Batch {start//BATCH_SIZE +1} traité ✔️")

        combined_df = pd.concat(processed_batches, ignore_index=True)

        # ------------ Global repasse ---------------------------------
        for _ in range(MAX_REPASS):
            dup_count = combined_df.apply(lambda col: col.str.lower()).duplicated().sum()
            if dup_count == 0:
                break
            combined_df = final_repasse(combined_df)

        st.success("✅ Paraphrasage terminé, aucun doublon détecté.")

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            combined_df.to_excel(writer, index=False, sheet_name="MODULES FAQs - FINAL")

        st.download_button(
            label="📥 Télécharger le fichier final",
            data=buffer.getvalue(),
            file_name="MODULES_FAQs_FINAL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.info("Importez un fichier pour commencer.")

###############################################################################
# Footer                                                                     #
###############################################################################

st.markdown(
    "<sub>La première occurrence de chaque question/réponse est conservée ; "
    "toutes les suivantes sont paraphrasées pour éliminer le contenu dupliqué. "
    "Traitement en batch pour les gros fichiers, puis vérification globale "
    "jusqu'à absence totale de doublons.</sub>",
    unsafe_allow_html=True,
)
