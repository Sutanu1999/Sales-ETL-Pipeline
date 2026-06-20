"""
Sales Data Warehouse Dashboard
Streamlit app reading from the MySQL gold-layer warehouse (sales_dw).
"""

import os
import pandas as pd
import streamlit as st
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Sales Data Pipeline Dashboard", layout="wide")


@st.cache_resource
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=os.getenv("MYSQL_PORT", "3306"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn)


st.title("Sales Data Pipeline Dashboard")
st.caption("PySpark + Delta Lake + Airflow + MySQL — Olist Brazilian E-commerce dataset")

# ---------- KPI Row ----------
kpi_query = """
    SELECT
        ROUND(SUM(price + freight_value), 2) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(price + freight_value) / COUNT(DISTINCT order_id), 2) AS avg_order_value
    FROM fact_order_items
"""
kpi_df = run_query(kpi_query)

avg_review_query = "SELECT ROUND(AVG(review_score), 2) AS avg_review FROM fact_reviews"
avg_review_df = run_query(avg_review_query)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"R$ {kpi_df['total_revenue'][0]:,.2f}")
col2.metric("Total Orders", f"{int(kpi_df['total_orders'][0]):,}")
col3.metric("Avg Order Value", f"R$ {kpi_df['avg_order_value'][0]:,.2f}")
col4.metric("Avg Review Score", f"{avg_review_df['avg_review'][0]} / 5")

st.divider()

# ---------- Monthly Revenue Trend ----------
st.subheader("Monthly Revenue Trend")
revenue_trend_query = """
    SELECT
        CONCAT(d.year, '-', LPAD(d.month, 2, '0')) AS period,
        ROUND(SUM(f.price + f.freight_value), 2) AS total_revenue
    FROM fact_order_items f
    JOIN dim_date d ON f.order_date_id = d.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
"""
revenue_df = run_query(revenue_trend_query)
st.line_chart(revenue_df.set_index("period")["total_revenue"])

st.divider()

# ---------- Two-column section: Top Categories + Revenue by State ----------
left, right = st.columns(2)

with left:
    st.subheader("Top 10 Categories by Revenue")
    category_query = """
        SELECT
            p.category_name_english AS category,
            ROUND(SUM(f.price), 2) AS revenue
        FROM fact_order_items f
        JOIN dim_product p ON f.product_id = p.product_id
        WHERE p.category_name_english IS NOT NULL
        GROUP BY p.category_name_english
        ORDER BY revenue DESC
        LIMIT 10
    """
    category_df = run_query(category_query)
    st.bar_chart(category_df.set_index("category")["revenue"])

with right:
    st.subheader("Top 10 States by Revenue")
    state_query = """
        SELECT
            c.customer_state AS state,
            ROUND(SUM(f.price + f.freight_value), 2) AS revenue
        FROM fact_order_items f
        JOIN dim_customer c ON f.customer_id = c.customer_id
        GROUP BY c.customer_state
        ORDER BY revenue DESC
        LIMIT 10
    """
    state_df = run_query(state_query)
    st.bar_chart(state_df.set_index("state")["revenue"])

st.divider()

# ---------- Payment Type + Order Status ----------
left2, right2 = st.columns(2)

with left2:
    st.subheader("Payment Type Distribution")
    payment_query = """
        SELECT payment_type, COUNT(*) AS num_payments
        FROM fact_payments
        GROUP BY payment_type
        ORDER BY num_payments DESC
    """
    payment_df = run_query(payment_query)
    st.bar_chart(payment_df.set_index("payment_type")["num_payments"])

with right2:
    st.subheader("Order Status Breakdown")
    status_query = """
        SELECT order_status, COUNT(DISTINCT order_id) AS num_orders
        FROM fact_order_items
        GROUP BY order_status
        ORDER BY num_orders DESC
    """
    status_df = run_query(status_query)
    st.bar_chart(status_df.set_index("order_status")["num_orders"])

st.divider()
st.caption("Data source: sales_dw MySQL warehouse, loaded via PySpark + Delta Lake + Airflow pipeline.")