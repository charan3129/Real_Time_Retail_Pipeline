"""
Streamlit Retail Analytics Dashboard.
Connects to Snowflake Gold layer for daily revenue, top products,
store performance, real-time transaction count, and interactive filters.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import snowflake.connector
from config import SNOWFLAKE_CONFIG, DASHBOARD_CONFIG

st.set_page_config(page_title=DASHBOARD_CONFIG["page_title"], layout=DASHBOARD_CONFIG["layout"])

@st.cache_resource
def get_connection():
    try:
        return snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None

@st.cache_data(ttl=DASHBOARD_CONFIG["refresh_interval_seconds"])
def query(sql):
    conn = get_connection()
    if not conn: return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# Sidebar filters
st.sidebar.title("Filters")
dr = st.sidebar.date_input("Date Range", value=((datetime.now()-timedelta(days=30)).date(), datetime.now().date()))
start, end = (dr[0], dr[1]) if len(dr)==2 else ((datetime.now()-timedelta(30)).date(), datetime.now().date())

regions_df = query("SELECT DISTINCT region FROM ANALYTICS.DIM_STORE ORDER BY region")
regions = regions_df["REGION"].tolist() if not regions_df.empty else []
sel_regions = st.sidebar.multiselect("Region", regions, default=regions)
rf = f"AND s.region IN ({','.join(repr(r) for r in sel_regions)})" if sel_regions else ""

# Header + KPIs
st.title("Retail Analytics Dashboard")
kpi = query(f"""SELECT COUNT(DISTINCT f.order_id) AS orders, SUM(f.total_amount) AS revenue,
    AVG(f.total_amount) AS aov, SUM(f.quantity) AS units
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key=d.date_key
JOIN ANALYTICS.DIM_STORE s ON f.store_key=s.store_key
WHERE d.full_date BETWEEN '{start}' AND '{end}' {rf}""")
if not kpi.empty:
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Orders", f"{kpi['ORDERS'].iloc[0]:,.0f}")
    c2.metric("Revenue", f"${kpi['REVENUE'].iloc[0]:,.2f}")
    c3.metric("Avg Order", f"${kpi['AOV'].iloc[0]:,.2f}")
    c4.metric("Units", f"{kpi['UNITS'].iloc[0]:,.0f}")

# Revenue trend
st.subheader("Daily Revenue")
rev = query(f"""SELECT d.full_date, SUM(f.total_amount) AS rev
FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_DATE d ON f.date_key=d.date_key
JOIN ANALYTICS.DIM_STORE s ON f.store_key=s.store_key
WHERE d.full_date BETWEEN '{start}' AND '{end}' {rf}
GROUP BY d.full_date ORDER BY d.full_date""")
if not rev.empty:
    fig = go.Figure(go.Scatter(x=rev["FULL_DATE"], y=rev["REV"], mode="lines+markers"))
    fig.update_layout(template="plotly_white", height=400)
    st.plotly_chart(fig, use_container_width=True)

# Top products + Store performance
l, r = st.columns(2)
with l:
    st.subheader("Top 10 Products")
    prods = query(f"""SELECT p.product_name, p.category, SUM(f.total_amount) AS rev
    FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_PRODUCT p ON f.product_key=p.product_key AND p.is_current=TRUE
    JOIN ANALYTICS.DIM_DATE d ON f.date_key=d.date_key
    WHERE d.full_date BETWEEN '{start}' AND '{end}'
    GROUP BY p.product_name, p.category ORDER BY rev DESC LIMIT 10""")
    if not prods.empty:
        st.plotly_chart(px.bar(prods, x="REV", y="PRODUCT_NAME", orientation="h", color="CATEGORY",
            height=400, template="plotly_white").update_layout(yaxis=dict(autorange="reversed")), use_container_width=True)

with r:
    st.subheader("Store Performance")
    stores = query(f"""SELECT s.store_name, s.region, SUM(f.total_amount) AS rev
    FROM ANALYTICS.FACT_SALES f JOIN ANALYTICS.DIM_STORE s ON f.store_key=s.store_key
    JOIN ANALYTICS.DIM_DATE d ON f.date_key=d.date_key
    WHERE d.full_date BETWEEN '{start}' AND '{end}' {rf}
    GROUP BY s.store_name, s.region ORDER BY rev DESC LIMIT 15""")
    if not stores.empty:
        st.plotly_chart(px.bar(stores, x="STORE_NAME", y="REV", color="REGION",
            height=400, template="plotly_white").update_layout(xaxis_tickangle=-45), use_container_width=True)

# 24h volume
st.subheader("Transaction Volume (Last 24h)")
rt = query("""SELECT DATE_TRUNC('HOUR', transaction_timestamp) AS hr, COUNT(*) AS cnt
FROM ANALYTICS.FACT_SALES WHERE transaction_timestamp >= DATEADD('DAY',-1,CURRENT_TIMESTAMP())
GROUP BY hr ORDER BY hr""")
if not rt.empty:
    st.plotly_chart(go.Figure(go.Bar(x=rt["HR"], y=rt["CNT"], marker_color="#4CAF50"))
        .update_layout(template="plotly_white", height=350), use_container_width=True)

st.markdown(f"---\n*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
