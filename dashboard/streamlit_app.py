import streamlit as st
import requests
import time
import os

API_BASE = os.getenv("API_BASE", "http://dashboard:8000")

st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("Crypto Real-Time Dashboard")

refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 1, 10, 2)

def fetch_data():
    try:
        prices_resp = requests.get(f"{API_BASE}/prices").json()
        alerts_resp = requests.get(f"{API_BASE}/alerts").json()
        return prices_resp, alerts_resp
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return [], []

# Auto-refresh every N seconds without recursion
placeholder = st.empty()

while True:
    with placeholder.container():
        prices, alerts = fetch_data()

        st.subheader("Latest Prices & Moving Averages")
        cols = st.columns(len(prices)) if prices else []
        for col, item in zip(cols, prices):
            col.metric(
                label=item["symbol"].upper(),
                value=f"{item['latest_price']:.2f}",
                delta=f"{item['moving_average']:.2f}"
            )

        st.subheader("Recent Alerts")
        for alert in reversed(alerts[-10:]):
            st.write(
                f"{alert['symbol'].upper()} | Price: {alert['price']:.2f} | "
                f"MA: {alert['ma']:.2f} | Δ%: {alert['delta_pct']}% | "
                f"TS: {time.strftime('%H:%M:%S', time.localtime(alert['ts']))}"
            )

    time.sleep(refresh_interval)
