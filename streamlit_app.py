import streamlit as st
import openai, gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.faq_generator import generate_faq_pairs   # logique séparée

st.set_page_config(page_title="FAQs Generator", page_icon="🤖")

# --- 1) Secrets & clients ----------------------------------------------------
openai.api_key = st.secrets["OPENAI_API_KEY"]

scope = ["https://www.googleapis.com/auth/spreadsheets"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    st.secrets["gcp_service_account"], scope)
gs = gspread.authorize(creds)

# Paramètres (peuvent être saisis dans la sidebar)
sheet_id   = st.text_input("ID du Google Sheet", st.secrets.get("sheet_id", ""))
src_name   = st.text_input("Onglet source (SRC)", "MODULES FAQs ENRICHIES")
dest_name  = st.text_input("Onglet destination (DEST)", "MODULES FAQs - FINAL")
max_pairs  = st.slider("Nombre de paires à générer", 1, 8, 8)

if st.button("Lancer la génération"):
    try:
        # 2) Lecture / génération
        sh   = gs.open_by_key(sheet_id)
        src  = sh.worksheet(src_name)
        dest = generate_faq_pairs(src, dest_name, max_pairs)
        st.success(f"✅ Onglet « {dest_name} » mis à jour ({dest.row_count} lignes).")
        st.balloons()
    except Exception as e:
        st.error(f"Erreur : {e}")
