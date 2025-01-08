import streamlit as st
from pathlib import Path
import logging
import tempfile
from io import StringIO
from newsletter.utils import read_contact_file
# Page config
st.set_page_config(
    page_title="Contact Processor",
    page_icon="📝",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# Hide streamlit elements
hide_streamlit_style = """
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    [data-testid="stSidebar"][aria-expanded="true"] {display: none;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Center all content
st.markdown("""
<style>
    .stApp {
        max-width: 800px;
        margin: 0 auto;
    }
    .uploadedFile {
        max-width: 400px;
        margin: 0 auto;
    }
    div[data-testid="stDownloadButton"] {
        text-align: center;
    }
    h1, h2, h3, p {
        text-align: center;
    }
    .stMarkdown {
        text-align: center;
    }
    div.stMarkdown > div {
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Set up logging
log_output = StringIO()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(log_output),
        logging.FileHandler("contact_ingester.log")
    ]
)
logger = logging.getLogger(__name__)

# Logo/Header
col1, col2, col3 = st.columns([1,2,1])
with col2:
    st.image("docs/logo.png", use_container_width=True)
    pass

st.title("Contact List Processor")
st.markdown("Process and deduplicate contact lists in seconds.")

def display_logs():
    """Display logs in the Streamlit interface"""
    log_contents = log_output.getvalue()
    if log_contents:
        st.text_area("Processing Logs", log_contents, height=300)
    log_output.seek(0)
    log_output.truncate(0)

# ... existing read_contact_file function stays the same ...

def process_uploaded_files(uploaded_files):
    """Process multiple uploaded files"""
    if not uploaded_files:
        st.warning("Please upload one or more CSV files")
        return
    
    processed_files = []
    
    # Use temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        
        for uploaded_file in uploaded_files:
            # Save uploaded file to temp directory
            temp_path = temp_dir / uploaded_file.name
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            st.write(f"Processing: {uploaded_file.name}")
            df = read_contact_file(str(temp_path))
            
            if df is not None:
                # Get the path of the generated Excel file
                excel_path = temp_path.with_suffix('.xlsx')
                if excel_path.exists():
                    # Store file content for download
                    with open(excel_path, "rb") as f:
                        processed_files.append({
                            "name": excel_path.name,
                            "data": f.read()
                        })
                st.success(f"Successfully processed {uploaded_file.name}")
            
            display_logs()
        
        # Add download buttons for processed files
        if processed_files:
            st.markdown("### Download Processed Files")
            for file_info in processed_files:
                st.download_button(
                    label=f"Download {file_info['name']}",
                    data=file_info['data'],
                    file_name=file_info['name'],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

def main():
    # File uploader
    uploaded_files = st.file_uploader(
        "Upload Constant Contact CSV files to process",
        type="csv",
        accept_multiple_files=True
    )
    
    # Process button
    if uploaded_files:
        if st.button("Process Files", type="primary"):
            process_uploaded_files(uploaded_files)

if __name__ == "__main__":
    main()
