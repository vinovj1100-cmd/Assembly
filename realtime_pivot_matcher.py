import streamlit as st
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
import seaborn as sns
import json
import sqlite3
from datetime import datetime
from collections import defaultdict
import argparse

st.set_page_config(
    page_title="Real-Time Pivot Table Matcher",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def display_header():
    """Display app header and introduction"""
    st.markdown("""
    <div style='text-align: center; padding: 20px; background-color: #f0f2f6; border-radius: 10px;'>
        <h1 style='color: #1f77b4; margin-bottom: 10px;'>📊 Real-Time Pivot Table Matching System</h1>
        <p style='color: #666;'>Advanced data matching and analysis with live visualization</p>
    </div>
    """, unsafe_allow_html=True)

def create_sidebar() -> Dict:
    """Create sidebar with configuration options"""
    st.sidebar.header("⚙️ Configuration")
    
    # Data source options
    st.sidebar.subheader("📊 Data Sources")
    source_type = st.sidebar.selectbox("Select Data Source", 
                                      ["demo", "csv", "excel", "database"])
    
    if source_type == "csv":
        file_path = st.sidebar.text_input("CSV File Path", 
                                         "data/sales_data.csv")
    elif source_type == "excel":
        file_path = st.sidebar.text_input("Excel File Path", 
                                         "data/metrics.xlsx")
    elif source_type == "database":
        file_path = st.sidebar.text_input("Database Path", 
                                         "sqlite:data/sales.db")
    else:
        file_path = None
    
    # Processing options
    st.sidebar.subheader("🔄 Processing Options")
    update_interval = st.sidebar.slider("Update Interval (seconds)", 
                                       1, 60, 5)
    max_iterations = st.sidebar.slider("Max Iterations", 
                                      1, 20, 10)
    
    return {
        'source_type': source_type,
        'file_path': file_path,
        'update_interval': update_interval,
        'max_iterations': max_iterations
    }

def load_data(source_type: str, file_path: str) -> pd.DataFrame:
    """Load data based on source type"""
    try:
        if source_type == "demo":
            # Generate demo data
            return generate_demo_data(1000)
        elif source_type == "csv":
            return pd.read_csv(file_path)
        elif source_type == "excel":
            return pd.read_excel(file_path)
        elif source_type == "database":
            conn_str = file_path.replace('sqlite:', '')
            conn = sqlite3.connect(conn_str)
            query = f"SELECT * FROM {file_path.split('/')[-1]}"
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def generate_demo_data(rows: int = 1000) -> pd.DataFrame:
    """Generate sample data for demo"""
    np.random.seed(int(time.time()))
    
    data = {
        'date': pd.date_range(start='2024-01-01', periods=rows, freq='H'),
        'product_id': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005'], rows),
        'category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Sports'], rows),
        'region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], rows),
        'sales': np.random.normal(1000, 300, rows).clip(0).round(2),
        'quantity': np.random.poisson(50, rows),
        'profit': np.random.normal(100, 30, rows).clip(0).round(2),
        'customer_id': [f'C{str(i).zfill(4)}' for i in np.random.randint(1, 1001, rows)],
        'channel': np.random.choice(['Online', 'Store', 'Phone', 'Mobile'], rows),
        'discount': np.random.uniform(0, 0.3, rows).round(2)
    }
    
    return pd.DataFrame(data)

def create_pivot_table(data: pd.DataFrame, pivot_config: Dict) -> pd.DataFrame:
    """Create pivot table from data"""
    try:
        required_cols = pivot_config['rows'] + pivot_config['columns'] + pivot_config['values']
        for col in required_cols:
            if col not in data.columns:
                st.warning(f"Column '{col}' not found in data")
                return pd.DataFrame()
        
        pivot_table = pd.pivot_table(
            data,
            values=pivot_config['values'],
            index=pivot_config['rows'],
            columns=pivot_config['columns'],
            aggfunc='sum',
            fill_value=0,
            margins=True,
            margins_name='Total'
        )
        
        if ('sales', 'Total') in pivot_table.columns and ('profit', 'Total') in pivot_table.columns:
            pivot_table['sales_profit_ratio'] = (
                pivot_table[('sales', 'Total')] / pivot_table[('profit', 'Total')].replace(0, np.nan)
            ).fillna(0)
        
        return pivot_table
    
    except Exception as e:
        st.error(f"Error creating pivot table: {str(e)}")
        return pd.DataFrame()

def visualize_pivot_results(pivot_table: pd.DataFrame, title: str):
    """Create visualizations for pivot table"""
    try:
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle(title, fontsize=16, fontweight='bold')
        
        # Sales by Category and Region
        if ('sales', 'Total') in pivot_table.columns:
            pivot_sales = pivot_table[('sales', 'Total')]
            if 'region' in pivot_sales.index.names:
                pivot_sales.unstack('region').plot(kind='bar', ax=axes[0, 0], colormap='viridis')
                axes[0, 0].set_title('Sales by Category and Region')
                axes[0, 0].set_xlabel('Category')
                axes[0, 0].set_ylabel('Sales')
        
        # Profit Trend
        if ('profit', 'Total') in pivot_table.columns:
            pivot_profit = pivot_table[('profit', 'Total')]
            pivot_profit.unstack('region').T.plot(kind='line', ax=axes[0, 1], marker='o')
            axes[0, 1].set_title('Profit Trend by Region')
            axes[0, 1].set_xlabel('Year-Quarter')
            axes[0, 1].set_ylabel('Profit')
        
        # Quantity Distribution
        if ('quantity', 'Total') in pivot_table.columns:
            pivot_qty = pivot_table[('quantity', 'Total')]
            pivot_qty.unstack('region').plot(kind='pie', ax=axes[1, 0], autopct='%1.1f%%')
            axes[1, 0].set_title('Quantity Distribution by Region')
            axes[1, 0].set_ylabel('')
        
        # Correlation Heatmap
        numeric_cols = [(col, 'Sales') if col == 'sales' else 
                       ((col, 'Quantity') if col == 'quantity' else
                        ((col, 'Profit') if col == 'profit' else col))
                       for col in pivot_table.columns]
        
        if len(numeric_cols) >= 2:
            correlation_data = pivot_table[numeric_cols].T.corr()
            sns.heatmap(correlation_data, annot=True, cmap='coolwarm', center=0, ax=axes[1, 1])
            axes[1, 1].set_title('Metric Correlations')
        
        plt.tight_layout()
        return fig
    
    except Exception as e:
        st.error(f"Error creating visualization: {str(e)}")
        return None

def match_records(current_data: pd.DataFrame, reference_data: pd.DataFrame) -> Dict:
    """Match records between datasets"""
    try:
        matching_config = {
            'similarity_threshold': 0.8,
            'match_columns': ['product_id', 'date', 'customer_id'],
            'fuzzy_match': True,
            'weight_columns': {'sales': 0.5, 'quantity': 0.3, 'profit': 0.2}
        }
        
        metrics = defaultdict(list)
        matches = []
        
        # Exact matching
        exact_matches = []
        for idx, row in current_data.iterrows():
            match_found = False
            for ref_idx, ref_row in reference_data.iterrows():
                match_score = 0
                for col in matching_config['match_columns']:
                    if col in row and col in ref_row:
                        if str(row[col]) == str(ref_row[col]):
                            match_score += matching_config.get('weight_columns', {}).get(col, 1)
                
                if match_score > 0:
                    exact_matches.append({
                        'current_index': idx,
                        'reference_index': ref_idx,
                        'match_score': match_score,
                        'match_type': 'exact'
                    })
                    match_found = True
                    break
        
        # Calculate metrics
        metrics['total_current_records'] = len(current_data)
        metrics['total_reference_records'] = len(reference_data)
        metrics['exact_matches'] = len(exact_matches)
        metrics['match_rate'] = len(exact_matches) / len(current_data) if len(current_data) > 0 else 0
        
        return {
            'metrics': dict(metrics),
            'matches': exact_matches,
            'similarity_score': 0.0  # Simplified for demo
        }
    
    except Exception as e:
        st.error(f"Error matching records: {str(e)}")
        return {'metrics': {}, 'matches': [], 'similarity_score': 0.0}

def main():
    """Main Streamlit app"""
    display_header()
    
    # Sidebar configuration
    config = create_sidebar()
    
    # Streamlit session state
    if 'pivot_tables' not in st.session_state:
        st.session_state.pivot_tables = []
    if 'match_results' not in st.session_state:
        st.session_state.match_results = []
    
    # Control buttons
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if st.button("🔄 Process Data", type="primary"):
            with st.spinner("Processing data..."):
                # Load data
                current_data = load_data(config['source_type'], config['file_path'])
                reference_data = load_data("demo", None)  # Always use demo reference
                
                if not current_data.empty:
                    # Create pivot table
                    pivot_config = {
                        'rows': ['category', 'region'],
                        'columns': ['date'],
                        'values': ['sales', 'quantity', 'profit']
                    }
                    
                    pivot_table = create_pivot_table(current_data, pivot_config)
                    
                    if not pivot_table.empty:
                        # Store and display results
                        st.session_state.pivot_tables.append(pivot_table)
                        st.session_state.match_results.append(
                            match_records(current_data, reference_data)
                        )
                        
                        # Display fresh results
                        st.subheader("📊 Current Pivot Table")
                        st.dataframe(pivot_table.head(10), use_container_width=True)
                        
                        # Visualization
                        fig = visualize_pivot_results(pivot_table, "Live Pivot Analysis")
                        if fig:
                            st.pyplot(fig)
                        
                        # Matching results
                        st.subheader("🔍 Matching Results")
                        results = st.session_state.match_results[-1]
                        st.metric("Match Rate", f"{results['metrics'].get('match_rate', 0):.2%}")
                        st.metric("Total Matches", results['metrics'].get('exact_matches', 0))
    
    with col2:
        st.subheader("📈 Recent Results")
        
        if st.session_state.pivot_tables:
            # Display recent pivot tables
            recent_pivot = st.session_state.pivot_tables[-1]
            st.write("**Latest Pivot Table Sample:**")
            st.dataframe(recent_pivot.head(5), use_container_width=True)
        
        if st.session_state.match_results:
            # Display recent match results
            recent_match = st.session_state.match_results[-1]
            st.write("**Recent Match Summary:**")
            st.json(recent_match['metrics'])
        
        # Clear button
        if st.button("🧹 Clear All Results"):
            st.session_state.pivot_tables = []
            st.session_state.match_results = []
            st.experimental_rerun()

if __name__ == "__main__":
    main()
