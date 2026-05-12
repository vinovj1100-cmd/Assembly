import streamlit as st
import pytesseract
import pypdf
import re
import io
import requests
import pandas as pd
import json
import os
from datetime import datetime
import hashlib
from pdf2image import convert_from_bytes
from pyzbar.pyzbar import decode
from deep_translator import GoogleTranslator
import base64
from cryptography.fernet import Fernet
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ CONFIG CONSTANTS ------------------
EXPORT_FORMAT_OPTIONS = ["CSV", "Excel", "JSON", "PDF Report"]

# ------------------ 1. SECURITY ------------------
ENCRYPTION_KEY_ENV = os.getenv('OZON_ENCRYPTION_KEY')
if ENCRYPTION_KEY_ENV:
    ENCRYPTION_KEY = ENCRYPTION_KEY_ENV.encode() if isinstance(ENCRYPTION_KEY_ENV, str) else ENCRYPTION_KEY_ENV
    cipher_suite = Fernet(ENCRYPTION_KEY)
else:
    logger.warning("OZON_ENCRYPTION_KEY not set; generating an ephemeral encryption key.")
    ENCRYPTION_KEY = Fernet.generate_key()
    cipher_suite = Fernet(ENCRYPTION_KEY)

def encrypt_data(data):
    return cipher_suite.encrypt(data.encode()).decode()

def decrypt_data(encrypted_data):
    return cipher_suite.decrypt(encrypted_data.encode()).decode()

# ------------------ 2. PDF PROCESSING ------------------
def extract_text_from_pdf(pdf_bytes):
    try:
        pdf_reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        st.error(f"❌ Error extracting text from PDF: {str(e)}")
        return None

def parse_barcode_from_pdf(pdf_bytes):
    try:
        images = convert_from_bytes(pdf_bytes)
        all_data = []
        for i, image in enumerate(images):
            text = pytesseract.image_to_string(image)
            tracking_numbers = re.findall(r'\b\d{4,12}-?\d{4}-?\d?\b', text)
            if tracking_numbers:
                for tn in tracking_numbers:
                    all_data.append({
                        'tracking_id': tn,
                        'page': i + 1,
                        'raw_text': text[:200]
                    })
        return all_data
    except Exception as e:
        st.error(f"❌ Error processing PDF: {str(e)}")
        return None

# ------------------ 3. EXPORT FUNCTIONALITY ------------------
def export_data(data, format_type, operator_name, prefix="ozon_export"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{operator_name}_{timestamp}"

    try:
        fmt = (format_type or "").strip().lower()

        if fmt == "csv":
            if isinstance(data, dict):
                df = pd.DataFrame.from_dict(data, orient='index')
            else:
                df = pd.DataFrame(data)
            csv = df.to_csv(index=False)
            st.download_button(label="Download CSV", data=csv, file_name=f"{filename}.csv", mime="text/csv")

        elif fmt in ("excel", "xlsx"):
            if isinstance(data, dict):
                with pd.ExcelWriter(f"{filename}.xlsx") as writer:
                    for key, value in data.items():
                        if isinstance(value, list):
                            df = pd.DataFrame(value)
                            df.to_excel(writer, sheet_name=key.replace('_', ' ')[:31], index=False)
                        else:
                            df = pd.DataFrame(data)
                            df.to_excel(writer, sheet_name="Data", index=False)
            else:
                df = pd.DataFrame(data)
                df.to_excel(f"{filename}.xlsx", index=False)

            with open(f"{filename}.xlsx", 'rb') as f:
                st.download_button(label="Download Excel", data=f.read(), file_name=f"{filename}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        elif fmt == "json":
            json_data = json.dumps(data, indent=2, default=str)
            st.download_button(label="Download JSON", data=json_data, file_name=f"{filename}.json", mime="application/json")

    except Exception as e:
        st.error(f"❌ Export failed: {str(e)}")

# ==================== UTILITY FUNCTIONS ====================
def robust_parse_multiline(text_data):
    SCANNING_ID_REGEX = re.compile(r"\b\d{4,12}-?\d{4}-?\d?\b")
    data_map = {}
    current_tn = None

    for line in text_data.strip().split('\n'):
        line = line.strip()
        if not line: continue

        tn_match = SCANNING_ID_REGEX.search(line)
        if tn_match:
            current_tn = tn_match.group()
            desc = line.replace(current_tn, "").strip('|').strip()
            data_map.setdefault(current_tn, set())
            if desc: data_map[current_tn].add(desc)
        elif current_tn:
            data_map[current_tn].add(line)
    return data_map

def standardize_title(raw_text):
    text = raw_text.upper().replace("SMARTPHONE ", "").replace("MOBILE PHONE ", "")
    mappings = {
        "IPHONE": "APPLE IPHONE", " ORANGE": " COSMIC ORANGE", 
        " BLUE": " DEEP BLUE", " GRAY": " TITAN GRAY", 
        " GREY": " TITAN GRAY", " PURPLE": " SANDY PURPLE"
    }
    for key, value in mappings.items():
        if key in text and value not in text:
            text = text.replace(key, value)
    return text.strip()

# ------------------ 4. MAIN APPLICATION & SESSION INIT ------------------
if 'audit_results' not in st.session_state:
    st.session_state.audit_results = []
if 'translation_history' not in st.session_state:
    st.session_state.translation_history = []

# Mock Inventory Database
if 'inventory_db' not in st.session_state:
    st.session_state.inventory_db = pd.DataFrame([
        {"SKU": "APP-IP15-256-BLK", "Product": "APPLE IPHONE 15 256GB BLACK", "Stock": 45, "Location": "A1-01"},
        {"SKU": "APP-IP15P-256-ORG", "Product": "APPLE IPHONE 15 PRO COSMIC ORANGE 256GB", "Stock": 8, "Location": "A1-02"},
        {"SKU": "SAM-S24-512-GRY", "Product": "SAMSUNG GALAXY S24 TITAN GRAY 512GB", "Stock": 12, "Location": "B2-15"},
        {"SKU": "POC-F6-512-BLK", "Product": "POCO F6 12/512GB BLACK", "Stock": 150, "Location": "C4-05"}
    ])

# Mock Daily Orders Database
if 'daily_orders' not in st.session_state:
    st.session_state.daily_orders = [
        {"Order ID": "ORD-9981", "Status": "Pending", "Required SKUs": ["APP-IP15P-256-ORG", "POC-F6-512-BLK"]},
        {"Order ID": "ORD-9982", "Status": "Pending", "Required SKUs": ["SAM-S24-512-GRY", "SAM-S24-512-GRY"]},
        {"Order ID": "ORD-9983", "Status": "Shipped", "Required SKUs": ["APP-IP15-256-BLK"]}
    ]

SCANNING_ID_REGEX = re.compile(r"\b\d{4,12}-?\d{4}-?\d?\b")

st.set_page_config(page_title="Ozon Master Tool Pro", layout="wide", page_icon="📦", initial_sidebar_state="expanded")

# Sidebar
with st.sidebar:
    st.image("https://icons8.com", width=80)
    st.title("Operator Settings")
    operator_name = st.text_input("Operator Name", value="Staff_01")
    
    st.divider()
    st.subheader("📷 Default Scanner Setup")
    sidebar_scan_dpi = st.select_slider("Global Resolution (DPI)", options=[150, 200, 300], value=300, key="sidebar_dpi")
    
    st.divider()
    st.subheader("💾 Export Settings")
    default_export_format = st.selectbox("Default Export Format", EXPORT_FORMAT_OPTIONS, index=0)

if 'session_hash' not in st.session_state or not st.session_state.session_hash:
    st.session_state.session_hash = hashlib.sha256(os.urandom(16)).hexdigest()[:16]

st.title(f"📦 Ozon Master Tool Pro | **{operator_name}**")

# TABS
tabs = st.tabs([
    "📊 Dashboard", "📦 Inventory Hub", "🛒 Pick & Pack", 
    "🔍 PDF Sort", "⚖️ Auditor", "🌐 Translator", "🔄 Bulk Convert", "📋 Export"
])

# --- TAB 1: DASHBOARD ---
with tabs[0]:
    st.subheader("📊 **Warehouse Command Center**")
    
    # Calculate live metrics
    total_stock = st.session_state.inventory_db['Stock'].sum()
    low_stock_items = len(st.session_state.inventory_db[st.session_state.inventory_db['Stock'] < 10])
    pending_orders = len([o for o in st.session_state.daily_orders if o['Status'] == 'Pending'])
    shipped_orders = len([o for o in st.session_state.daily_orders if o['Status'] == 'Shipped'])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📦 Total Items in Stock", total_stock)
    m2.metric("⚠️ Low Stock Alerts", low_stock_items, delta_color="inverse")
    m3.metric("⏳ Pending Orders", pending_orders)
    m4.metric("✅ Orders Shipped Today", shipped_orders)

    st.divider()
    st.markdown("#### 📡 API Status Checker")
    status_input = st.text_area("Paste Tracking Numbers to Check External Status", height=100)
    if st.button("Check API Status"):
        tracking_numbers = SCANNING_ID_REGEX.findall(status_input)
        if tracking_numbers:
            results = [{'Tracking ID': tn, 'Status': 'In Transit', 'Location': 'Moscow Hub', 'Updated': datetime.now().strftime('%H:%M')} for tn in tracking_numbers]
            st.dataframe(pd.DataFrame(results), use_container_width=True)
        else:
            st.warning("⚠️ No valid tracking numbers found.")

# --- TAB 2: INVENTORY HUB ---
with tabs[1]:
    st.subheader("📦 **Live Inventory Management**")
    
    # Low stock alert banner
    low_stock_df = st.session_state.inventory_db[st.session_state.inventory_db['Stock'] < 10]
    if not low_stock_df.empty:
        st.warning(f"⚠️ **ACTION REQUIRED:** {len(low_stock_df)} items are running low on stock (Less than 10 units).")
    
    # Display Editable Dataframe
    st.markdown("### Current Stock Levels")
    # Using data_editor allows the operator to manually update stock levels directly in the UI
    edited_df = st.data_editor(
        st.session_state.inventory_db, 
        use_container_width=True, 
        num_rows="dynamic",
        column_config={
            "Stock": st.column_config.NumberColumn("Stock", help="Current units in warehouse", min_value=0, step=1)
        }
    )
    # Save edits back to session state
    st.session_state.inventory_db = edited_df

# --- TAB 3: PICK & PACK ---
with tabs[2]:
    st.subheader("🛒 **Daily Fulfillment: Pick & Pack**")
    st.markdown("Select a pending order, scan the items placed in the box, and verify the pack.")

    # Filter pending orders
    pending_list = [o for o in st.session_state.daily_orders if o['Status'] == 'Pending']
    
    if not pending_list:
        st.success("🎉 All caught up! No pending orders to fulfill right now.")
    else:
        col_ord, col_scan = st.columns(2)
        
        with col_ord:
            selected_order_id = st.selectbox("Select Order to Pack", [o['Order ID'] for o in pending_list])
            current_order = next(o for o in pending_list if o['Order ID'] == selected_order_id)
            
            st.info(f"**Target Order:** {current_order['Order ID']}")
            st.write("**Required SKUs for this box:**")
            for sku in current_order['Required SKUs']:
                # Find product name for better UX
                prod_name = st.session_state.inventory_db.loc[st.session_state.inventory_db['SKU'] == sku, 'Product']
                p_label = prod_name.values[0] if not prod_name.empty else "Unknown SKU"
                st.markdown(f"- 📦 `{sku}` ({p_label})")

        with col_scan:
            scanned_skus_input = st.text_area("Barcode Scanner Input", placeholder="Scan items here (one per line)...", height=150)
            
            if st.button("✅ Verify & Pack Box", type="primary", use_container_width=True):
                scanned_list = [s.strip() for s in scanned_skus_input.split('\n') if s.strip()]
                required_list = current_order['Required SKUs'].copy()
                
                # Sort both to compare exactly
                if sorted(scanned_list) == sorted(required_list):
                    st.success("✅ **PACK MATCH!** The box is verified and ready to ship.")
                    
                    # Update Order Status
                    for idx, o in enumerate(st.session_state.daily_orders):
                        if o['Order ID'] == current_order['Order ID']:
                            st.session_state.daily_orders[idx]['Status'] = 'Shipped'
                            
                    # Deduct from Inventory
                    for sku in scanned_list:
                        st.session_state.inventory_db.loc[st.session_state.inventory_db['SKU'] == sku, 'Stock'] -= 1
                        
                    st.balloons()
                else:
                    st.error("❌ **PACK MISMATCH!** The scanned items do not match the required order.")
                    st.write(f"**Expected:** {sorted(required_list)}")
                    st.write(f"**Scanned:** {sorted(scanned_list)}")

# --- TAB 4: PDF SORT ---
with tabs[3]:
    st.subheader("🔍 **Pro PDF Label Sequencer**")
    col1, col2 = st.columns([1, 2])
    with col1:
        sort_list = st.text_area("🎯 Target Sequence Order", height=300, placeholder="Paste Tracking IDs here...")
    with col2:
        label_file = st.file_uploader("📄 Upload Labels PDF (Bulk)", type="pdf")
        with st.expander("⚙️ PDF Scanner Settings"):
            scan_dpi = st.select_slider("Resolution (DPI)", options=[150, 200, 300], value=200, key="tab2_dpi")
            use_ocr = st.checkbox("Enable OCR Fallback", value=True)

    if st.button("🚀 Scan, Sort & Generate PDF", type="primary", use_container_width=True):
        target_ids = [tid.strip() for tid in sort_list.split('\n') if tid.strip()]
        if not target_ids or not label_file:
            st.warning("⚠️ Provide sequence IDs and upload a PDF.")
        else:
            with st.spinner("Mapping PDF pages..."):
                try:
                    pdf_reader = pypdf.PdfReader(io.BytesIO(label_file.getvalue()))
                    pdf_writer = pypdf.PdfWriter()
                    images = convert_from_bytes(label_file.getvalue(), dpi=scan_dpi)
                    id_to_page_map = {}
                    
                    for i, img in enumerate(images):
                        page_codes = []
                        barcodes = decode(img)
                        for b in barcodes: page_codes.extend(SCANNING_ID_REGEX.findall(b.data.decode("utf-8")))
                        if not barcodes and use_ocr: page_codes.extend(SCANNING_ID_REGEX.findall(pytesseract.image_to_string(img)))
                        for code in set(page_codes): id_to_page_map[code] = pdf_reader.pages[i]

                    matched_count = 0
                    for tid in target_ids:
                        clean_tid_match = SCANNING_ID_REGEX.search(tid)
                        search_key = clean_tid_match.group() if clean_tid_match else tid
                        if search_key in id_to_page_map:
                            pdf_writer.add_page(id_to_page_map[search_key])
                            matched_count += 1

                    if matched_count > 0:
                        out_io = io.BytesIO()
                        pdf_writer.write(out_io)
                        st.success(f"✅ Created PDF with {matched_count} sorted pages!")
                        st.download_button("📥 Download SORTED_LABELS.pdf", out_io.getvalue(), "sorted_labels.pdf", "application/pdf")
                    else:
                        st.error("❌ No matches found.")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")

# --- TAB 5: AUDITOR ---
with tabs[4]:
    st.subheader("⚖️ **Verification Auditor**")
    col_a, col_b = st.columns(2)
    with col_a: master_in = st.text_area("**MASTER (Expected)**", height=300)
    with col_b: scan_in = st.text_area("**SCAN (Actual)**", height=300)

    if st.button("⚡ Run Discrepancy Analysis"):
        if master_in and scan_in:
            m_map, s_map = robust_parse_multiline(master_in), robust_parse_multiline(scan_in)
            results = []
            for tid in sorted(list(set(m_map.keys()) | set(s_map.keys()))):
                exp, got = m_map.get(tid, set()), s_map.get(tid, set())
                status = "✅ MATCH" if exp == got else "❌ ERROR"
                results.append({"Tracking ID": tid, "Status": status, "Expected": " | ".join(exp), "Actual": " | ".join(got)})
            
            df = pd.DataFrame(results)
            st.dataframe(df.style.apply(lambda x: ['background-color: #ffcccc' if '❌' in str(v) else '' for v in x], axis=1), use_container_width=True)

# --- TAB 6: TRANSLATOR ---
with tabs[5]:
    st.subheader("🌐 **Instant Translator**")
    col1, col2 = st.columns(2)
    with col1: source_lang = st.selectbox("Source", ["auto", "ru", "en", "zh-cn"])
    with col2: target_lang = st.selectbox("Target", ["en", "ru", "de", "es"])

    txt = st.text_area("Enter Text", height=150)
    if st.button("Translate Text"):
        try:
            res = GoogleTranslator(source=source_lang, target=target_lang).translate(txt)
            st.success("**Translation:** " + res)
            st.session_state.translation_history.append({'time': datetime.now().strftime('%H:%M:%S'), 'original': txt, 'translated': res})
        except Exception as e:
            st.error(f"Translation failed: {e}")

# --- TAB 7: BULK CONVERT ---
with tabs[6]:
    st.subheader("🔄 **Bulk Title Converter (White to Green)**")
    col_w, col_g = st.columns(2)
    with col_w: white_col = st.text_area("📄 Input (Original Titles)", height=300)
    
    if st.button("✨ Convert to Green Column", type="primary"):
        if white_col:
            lines = white_col.strip().split('\n')
            results = [standardize_title(GoogleTranslator(source='auto', target='en').translate(l)) if l.strip() else "" for l in lines]
            with col_g: st.text_area("✅ Output (Standardized)", value="\n".join(results), height=300)

# --- TAB 8: EXPORT ---
with tabs[7]:
    st.subheader("📋 **Data Export & Management**")
    export_format = st.selectbox("Select Export Format", EXPORT_FORMAT_OPTIONS, key="export_tab_format")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📥 Export Inventory Database"):
            export_data(st.session_state.inventory_db, export_format, operator_name, "inventory")
    with col2:
        if st.button("📥 Export Fulfillment Logs"):
            export_data(st.session_state.daily_orders, export_format, operator_name, "fulfillment")

    st.divider()
    if st.button("🗑️ Clear Local Session Data"):
        st.session_state.clear()
        st.rerun()
