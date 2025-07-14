import streamlit as st
from cassandra.cluster import Cluster
import pandas as pd
import time
from datetime import datetime

st.set_page_config(
    page_title="Crypto Data Dashboard",
    layout="wide"
)

# Custom styling
st.markdown("""
<style>
    table {
        width: 100%;
        border-collapse: collapse;
    }
    th {
        background-color: #0f172a;
        color: white;
        position: sticky;
        top: 0;
    }
    tr:nth-child(even) {
        background-color: #f2f2f2;
    }
    tr:hover {
        background-color: #ddd;
    }
    .last-update {
        position: fixed;
        bottom: 10px;
        right: 10px;
        background: #0f172a;
        color: white;
        padding: 5px 10px;
        border-radius: 5px;
    }
    .data-table {
        max-height: 70vh;
        overflow-y: auto;
    }
</style>
""", unsafe_allow_html=True)

def get_cassandra_session():
    try:
        cluster = Cluster(['localhost'])
        return cluster.connect('crypto')
    except Exception as e:
        st.error(f"Cassandra connection failed: {str(e)}")
        return None

def fetch_data(session):
    try:
        rows = session.execute("SELECT id, name, price_usd, processing_time FROM crypto.prices")
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"Data fetch failed: {str(e)}")
        return pd.DataFrame()

def convert_timestamp(ts):
    try:
        return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return "Invalid Timestamp"

def main():
    st.title("Real-time Cryptocurrency Prices")
    
    auto_refresh = st.sidebar.checkbox("Auto-refresh", True)
    refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 5, 60, 15)
    
    st.sidebar.subheader("Service Status")
    st.sidebar.markdown("**Spark UI**: [http://localhost:8080](http://localhost:8080)")
    st.sidebar.markdown("**Cassandra**: localhost:9042")
    st.sidebar.markdown("**Kafka**: localhost:9092")
    
    session = get_cassandra_session()
    if not session:
        return
    
    data_placeholder = st.empty()
    
    while True:
        df = fetch_data(session)
        
        if not df.empty:
            df['Processing Time'] = df['processing_time'].apply(convert_timestamp)
            df['Price (USD)'] = df['price_usd'].apply(lambda x: f"${x:,.4f}" if x else "N/A")
            
            display_df = df[['name', 'Price (USD)', 'Processing Time']]
            display_df.columns = ['Cryptocurrency', 'Price', 'Last Updated']
            

            display_df = display_df.sort_values('Last Updated', ascending=False)
        else:
            display_df = pd.DataFrame()
  
        with data_placeholder.container():
            if not display_df.empty:
                st.subheader(f"Showing {len(display_df)} records")
                st.markdown("""
                <div class="data-table">
                """ + display_df.to_html(index=False, classes="dataframe") + """
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("No data found in Cassandra")
        
        st.markdown(
            f"<div class='last-update'>Dashboard updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>",
            unsafe_allow_html=True
        )
        
        if not auto_refresh:
            break
        time.sleep(refresh_rate)

if __name__ == "__main__":
    main()