"""Streamlit Dashboard Configuration."""
import os
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_ANALYTICS"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}
DASHBOARD_CONFIG = {"page_title": "Retail Analytics", "page_icon": "cart", "layout": "wide", "refresh_interval_seconds": 300}
