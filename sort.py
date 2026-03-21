import streamlit as st
import fitz  # PyMuPDF
import io
from PIL import Image

st.set_page_config(page_title="PDF Row Sorter & Preview", layout="wide")
st.title("📄 PDF Row Sorter with Preview")

# Sidebar settings
uploaded_file = st.sidebar.file_uploader("1. Upload PDF", type="pdf")
tracking_input = st.sidebar.text_area("2. Paste Tracking Numbers in Order:", height=200)

st.sidebar.markdown("---")
st.sidebar.write("### 🛠 Adjust Row Capture")
top_cut = st.sidebar.slider("Top Cut (Higher = captures less above)", 0, 50, 15)
bottom_cut = st.sidebar.slider("Bottom Cut (Higher = captures more below)", 20, 100, 55)
row_spacing = st.sidebar.slider("Spacing between rows", 30, 120, 70)

if uploaded_file and tracking_input:
    doc = fitz.open(stream=uploaded_file.read(), filetype="pdf")
    first_page_rect = doc[0].rect
    ordered_list = [line.strip() for line in tracking_input.split('\n') if line.strip()]

    if st.button("🚀 Generate Preview & Sort", use_container_width=True):
        try:
            out_doc = fitz.open()
            row_data = {}
            
            # --- STEP 1: Find Row Data ---
            for page_idx in range(len(doc)):
                page = doc[page_idx]
                for tracking in ordered_list:
                    found_instances = page.search_for(tracking)
                    if found_instances:
                        found = found_instances[0] # Take first match
                        y0 = found.y0 - top_cut
                        y1 = found.y0 + bottom_cut
                        
                        row_data[tracking] = {
                            "page_idx": page_idx,
                            "rect": fitz.Rect(0, y0, page.rect.width, y1)
                        }

            # --- STEP 2: Rebuild PDF ---
            new_page = out_doc.new_page(width=first_page_rect.width, height=first_page_rect.height)
            current_y = 20 

            for tracking in ordered_list:
                if tracking in row_data:
                    data = row_data[tracking]
                    
                    if current_y + row_spacing > first_page_rect.height - 40:
                        new_page = out_doc.new_page(width=first_page_rect.width, height=first_page_rect.height)
                        current_y = 20

                    target_rect = fitz.Rect(0, current_y, first_page_rect.width, current_y + (data["rect"].y1 - data["rect"].y0))
                    new_page.show_pdf_page(target_rect, doc, data["page_idx"], clip=data["rect"])
                    current_y += row_spacing

            # --- STEP 3: Generate Output & Preview ---
            output_stream = io.BytesIO()
            out_doc.save(output_stream)
            pdf_bytes = output_stream.getvalue()

            # Generate Image Preview of Page 1
            st.subheader("👁 Preview (First Page)")
            preview_page = out_doc[0]
            pix = preview_page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5)) # High-res preview
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            st.image(img, caption="Sorted PDF Preview", use_container_width=True)
            
            # Download Button
            st.success("Looks good? Download below!")
            st.download_button("⬇️ Download Sorted PDF", data=pdf_bytes, file_name="clean_sorted.pdf", mime="application/pdf")

        except Exception as e:
            st.error(f"Error: {e}")
else:
    st.info("Upload a PDF and paste tracking numbers to start.")
