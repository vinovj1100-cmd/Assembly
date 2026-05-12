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

# Attempt to import Google Sheets dependencies
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------ CONFIG CONSTANTS ------------------
EXPORT_FORMAT_OPTIONS = ["CSV", "Excel", "JSON", "PDF Report"]

# ------------------ 2. GOOGLE SHEETS SYNC ENGINE ------------------
def init_gsheets_client(json_credentials_str):
    """Initialize Google Sheets client using Service Account JSON"""
    if not GSHEETS_AVAILABLE:
        st.error("Missing libraries. Please run: pip install gspread google-auth")
        return None
    
    try:
        creds_dict = json.loads(json_credentials_str)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"Failed to authenticate Google Sheets: {e}")
        return None

def push_to_gsheets(client, url, dataframe):
    """Overwrites the target Google Sheet with the current DataFrame"""
    try:
        sheet = client.open_by_url(url).sheet1
        sheet.clear()
        sheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
        return True
    except Exception as e:
        st.error(f"Failed to sync to Google Sheets: {e}")
        return False

def pull_from_gsheets(client, url):
    """Pulls data from Google Sheets into a Pandas DataFrame"""
    try:
        sheet = client.open_by_url(url).sheet1
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to pull from Google Sheets: {e}")
        return None

# ------------------ 3. PDF PROCESSING ------------------
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

# ------------------ 4. UTILITIES & DATA MAPPING ------------------
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

# ------------------ 5. INITIALIZATION & STATE MANAGEMENT ------------------
st.set_page_config(page_title="Ozon WMS Pro", layout="wide", page_icon="🏢", initial_sidebar_state="expanded")

if 'session_hash' not in st.session_state:
    st.session_state.session_hash = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
if 'audit_results' not in st.session_state:
    st.session_state.audit_results = []
if 'translation_history' not in st.session_state:
    st.session_state.translation_history = []

# WMS Default Databases (Used if GSheets are not linked)
if 'inventory_db' not in st.session_state:
    st.session_state.inventory_db = pd.DataFrame([
        {"SKU": "APP-IP15-256-BLK", "Product": "APPLE IPHONE 15 256GB BLACK", "Stock": 45, "Location": "A1-01"},
        {"SKU": "APP-IP15P-256-ORG", "Product": "APPLE IPHONE 15 PRO COSMIC ORANGE 256GB", "Stock": 8, "Location": "A1-02"},
        {"SKU": "SAM-S24-512-GRY", "Product": "SAMSUNG GALAXY S24 TITAN GRAY 512GB", "Stock": 12, "Location": "B2-15"}
    ])

if 'daily_orders' not in st.session_state:
    st.session_state.daily_orders = pd.DataFrame([
        {"Order ID": "ORD-9981", "Status": "Pending", "Required SKUs": "APP-IP15P-256-ORG, SAM-S24-512-GRY"},
        {"Order ID": "ORD-9982", "Status": "Pending", "Required SKUs": "SAM-S24-512-GRY"},
        {"Order ID": "ORD-9983", "Status": "Shipped", "Required SKUs": "APP-IP15-256-BLK"}
    ])

SCANNING_ID_REGEX = re.compile(r"\b\d{4,12}-?\d{4}-?\d?\b")

# ------------------ 6. SIDEBAR CONFIGURATION ------------------
with st.sidebar:
    st.title("🏢 WMS Operator")
    operator_name = st.text_input("Operator Name", value="Staff_01")
    
    st.divider()
    st.subheader("🔗 Google Sheets Sync")
    st.caption("Connect your cloud database for real-time tracking.")
    gsheet_json = st.text_area("Service Account JSON", help="wms-sync-bot@ozon-wms-app.iam.gserviceaccount.com")
    inventory_sheet_url = st.text_input("Inventory Sheet URL")
    orders_sheet_url = st.text_input("Orders Sheet URL")
    
    if st.button("🔄 Sync with Cloud", type="primary", use_container_width=True):
        if gsheet_json and inventory_sheet_url:
            with st.spinner("Syncing to Google Cloud..."):
                client = init_gsheets_client(gsheet_json)
                if client:
                    # Push local inventory to cloud
                    push_to_gsheets(client, inventory_sheet_url, st.session_state.inventory_db)
                    if orders_sheet_url:
                        push_to_gsheets(client, orders_sheet_url, st.session_state.daily_orders)
                    st.success("✅ Synced successfully!")
        else:
            st.warning("⚠️ Provide JSON credentials and at least one Sheet URL to sync.")

    st.divider()
    st.subheader("📷 Hardware Settings")
    scan_dpi = st.select_slider("Global Resolution (DPI)", options=[150, 200, 300], value=200, key="sidebar_dpi")
    st.text(f"Session ID: {st.session_state.session_hash}")

st.title(f"🏢 Ozon WMS Pro Dashboard | **{operator_name}**")

# ------------------ 7. TABS LAYOUT ------------------
tabs = st.tabs([
    "📊 Dashboard", "📥 Inbound Receiving", "📦 Inventory", "🛒 Pick & Pack", 
    "🔙 Returns", "🔍 PDF Sequencer", "⚖️ Auditor", "🔄 Bulk Convert"
])

# --- TAB 1: DASHBOARD ---
with tabs[0]:
    st.subheader("📊 **Warehouse Operations Center**")
    
    total_stock = st.session_state.inventory_db['Stock'].sum() if not st.session_state.inventory_db.empty else 0
    low_stock = len(st.session_state.inventory_db[st.session_state.inventory_db['Stock'] < 10]) if not st.session_state.inventory_db.empty else 0
    pending_orders = len(st.session_state.daily_orders[st.session_state.daily_orders['Status'] == 'Pending'])
    shipped_orders = len(st.session_state.daily_orders[st.session_state.daily_orders['Status'] == 'Shipped'])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📦 Total Items in Stock", total_stock)
    m2.metric("⚠️ Low Stock Alerts", low_stock, delta_color="inverse")
    m3.metric("⏳ Pending Orders", pending_orders)
    m4.metric("✅ Shipped Today", shipped_orders)

    st.divider()
    st.markdown("### 📡 Quick External Tracking")
    status_input = st.text_area("Paste External Tracking Numbers", height=100)
    if st.button("Check API Status"):
        tn_list = SCANNING_ID_REGEX.findall(status_input)
        if tn_list:
            results = [{'Tracking ID': tn, 'Status': 'In Transit', 'Location': 'Moscow Hub', 'Updated': datetime.now().strftime('%H:%M')} for tn in tn_list]
            st.dataframe(pd.DataFrame(results), use_container_width=True)

# --- TAB 2: INBOUND RECEIVING ---
with tabs[1]:
    st.subheader("📥 **Inbound Freight & Receiving**")
    st.markdown("Log new deliveries to add them to your warehouse inventory.")
    
    col_in1, col_in2, col_in3 = st.columns(3)
    with col_in1:
        inbound_sku = st.text_input("Scan / Enter SKU")
    with col_in2:
        inbound_qty = st.number_input("Quantity Received", min_value=1, value=1)
    with col_in3:
        inbound_bin = st.text_input("Assign to Bin Location", placeholder="e.g., C4-10")
        
    inbound_desc = st.text_input("Product Description (If New SKU)")

    if st.button("➕ Receive Inventory", type="primary"):
        if inbound_sku:
            inv = st.session_state.inventory_db
            if inbound_sku in inv['SKU'].values:
                # Update existing
                idx = inv.index[inv['SKU'] == inbound_sku].tolist()[0]
                inv.at[idx, 'Stock'] += inbound_qty
                if inbound_bin: inv.at[idx, 'Location'] = inbound_bin
                st.success(f"✅ Added {inbound_qty} units to existing SKU: {inbound_sku}")
            else:
                # Create new
                new_row = pd.DataFrame([{"SKU": inbound_sku, "Product": inbound_desc or "Unknown Product", "Stock": inbound_qty, "Location": inbound_bin or "UNASSIGNED"}])
                st.session_state.inventory_db = pd.concat([inv, new_row], ignore_index=True)
                st.success(f"✅ Created new SKU and received {inbound_qty} units.")
        else:
            st.error("Please enter a SKU.")

# --- TAB 3: INVENTORY HUB ---
with tabs[2]:
    st.subheader("📦 **Inventory Management**")
    low_stock_df = st.session_state.inventory_db[st.session_state.inventory_db['Stock'] < 10]
    if not low_stock_df.empty:
        st.warning(f"⚠️ **ACTION REQUIRED:** {len(low_stock_df)} SKUs require restocking.")
    
    st.markdown("### Master Stock List")
    edited_inv = st.data_editor(
        st.session_state.inventory_db, 
        use_container_width=True, 
        num_rows="dynamic",
        column_config={"Stock": st.column_config.NumberColumn("Stock", min_value=0, step=1)}
    )
    st.session_state.inventory_db = edited_inv

# --- TAB 4: PICK & PACK ---
with tabs[3]:
    st.subheader("🛒 **Fulfillment: Pick & Pack**")
    
    pending_df = st.session_state.daily_orders[st.session_state.daily_orders['Status'] == 'Pending']
    
    if pending_df.empty:
        st.success("🎉 All caught up! No pending orders.")
    else:
        col_ord, col_scan = st.columns(2)
        
        with col_ord:
            selected_order_id = st.selectbox("Select Order", pending_df['Order ID'].tolist())
            current_order = pending_df[pending_df['Order ID'] == selected_order_id].iloc[0]
            req_skus = [s.strip() for s in current_order['Required SKUs'].split(',')]
            
            st.info(f"**Packing Order:** {selected_order_id}")
            st.write("**Required SKUs:**")
            for sku in req_skus:
                prod_name = st.session_state.inventory_db.loc[st.session_state.inventory_db['SKU'] == sku, 'Product']
                p_label = prod_name.values[0] if not prod_name.empty else "Unknown SKU"
                st.markdown(f"- 📦 `{sku}` ({p_label})")

        with col_scan:
            scanned_skus_input = st.text_area("Barcode Scanner Input", placeholder="Scan items here (one per line)...", height=150)
            
            if st.button("✅ Verify & Ship", type="primary", use_container_width=True):
                scanned_list = [s.strip() for s in scanned_skus_input.split('\n') if s.strip()]
                
                if sorted(scanned_list) == sorted(req_skus):
                    st.success("✅ **MATCH!** Box verified and shipped.")
                    # Mark Shipped
                    st.session_state.daily_orders.loc[st.session_state.daily_orders['Order ID'] == selected_order_id, 'Status'] = 'Shipped'
                    # Deduct Inventory
                    for sku in scanned_list:
                        if sku in st.session_state.inventory_db['SKU'].values:
                            idx = st.session_state.inventory_db.index[st.session_state.inventory_db['SKU'] == sku].tolist()[0]
                            st.session_state.inventory_db.at[idx, 'Stock'] = max(0, st.session_state.inventory_db.at[idx, 'Stock'] - 1)
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ **MISMATCH!** Do not ship this box.")
                    st.write(f"Expected: {sorted(req_skus)}")
                    st.write(f"Scanned: {sorted(scanned_list)}")

# --- TAB 5: RETURNS ---
with tabs[4]:
    st.subheader("🔙 **Returns Processing**")
    st.markdown("Process inbound returns, mark order as returned, and place items back into active inventory.")
    
    ret_order = st.text_input("Original Order ID (Optional)")
    ret_sku = st.text_input("Scan Returned SKU")
    ret_reason = st.selectbox("Return Reason", ["Customer Cancelled", "Defective/Damaged", "Wrong Item Shipped", "Undeliverable"])
    
    if st.button("🔄 Process Return", type="primary"):
        if ret_sku:
            if ret_reason == "Defective/Damaged":
                st.warning(f"⚠️ Logged {ret_sku} as damaged. Item NOT added back to active inventory.")
            else:
                inv = st.session_state.inventory_db
                if ret_sku in inv['SKU'].values:
                    idx = inv.index[inv['SKU'] == ret_sku].tolist()[0]
                    inv.at[idx, 'Stock'] += 1
                    st.success(f"✅ Restocked 1 unit of {ret_sku}.")
                else:
                    st.info(f"SKU {ret_sku} not found in inventory. Please use Inbound Receiving to create it.")
            
            if ret_order and ret_order in st.session_state.daily_orders['Order ID'].values:
                st.session_state.daily_orders.loc[st.session_state.daily_orders['Order ID'] == ret_order, 'Status'] = 'Returned'
                st.success(f"Order {ret_order} status updated to 'Returned'.")
        else:
            st.error("Please scan a returning SKU.")

# --- TAB 6: PDF SEQUENCER ---
with tabs[5]:
    st.subheader("🔍 **Pro PDF Label Sequencer**")
    col1, col2 = st.columns([1, 2])
    with col1:
        sort_list = st.text_area("🎯 Target Sequence Order", height=300, placeholder="Paste Tracking IDs here...")
    with col2:
        label_file = st.file_uploader("📄 Upload Labels PDF (Bulk)", type="pdf")
        use_ocr = st.checkbox("Enable OCR Fallback", value=True)

    if st.button("🚀 Scan & Sort PDF", type="primary", use_container_width=True):
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
                        clean_tid = SCANNING_ID_REGEX.search(tid).group() if SCANNING_ID_REGEX.search(tid) else tid
                        if clean_tid in id_to_page_map:
                            pdf_writer.add_page(id_to_page_map[clean_tid])
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

# --- TAB 7: AUDITOR ---
with tabs[6]:
    st.subheader("⚖️ **Discrepancy Auditor**")
    col_a, col_b = st.columns(2)
    with col_a: master_in = st.text_area("**MASTER (Expected)**", height=200)
    with col_b: scan_in = st.text_area("**SCAN (Actual)**", height=200)

    if st.button("⚡ Run Discrepancy Analysis"):
        if master_in and scan_in:
            m_map, s_map = robust_parse_multiline(master_in), robust_parse_multiline(scan_in)
            results = []
            for tid in sorted(list(set(m_map.keys()) | set(s_map.keys()))):
                exp, got = m_map.get(tid, set()), s_map.get(tid, set())
                status = "✅ MATCH" if exp == got else "❌ ERROR"
                results.append({"ID": tid, "Status": status, "Expected": " | ".join(exp), "Actual": " | ".join(got)})
            
            st.dataframe(pd.DataFrame(results).style.apply(lambda x: ['background-color: #ffcccc' if '❌' in str(v) else '' for v in x], axis=1), use_container_width=True)

# --- TAB 8: BULK CONVERT ---
with tabs[7]:
    st.subheader("🔄 **Bulk Title Converter (White to Green)**")
    col_w, col_g = st.columns(2)
    with col_w: white_col = st.text_area("📄 Input (Original Titles)", height=300)
    
    if st.button("✨ Convert & Translate", type="primary"):
        if white_col:
            with st.spinner("Translating and formatting..."):
                lines = white_col.strip().split('\n')
                translator = GoogleTranslator(source='auto', target='en')
                results = []
                for l in lines:
                    if l.strip():
                        try:
                            results.append(standardize_title(translator.translate(l)))
                        except:
                            results.append(l.upper())
                    else:
                        results.append("")
                with col_g: st.text_area("✅ Output (Standardized)", value="\n".join(results), height=300)
