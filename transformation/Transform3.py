import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# 1. Connect
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    dbname="dataengineering_project",
    user="postgres",
    password="Ben080502"
)
cur = conn.cursor()

# 2. Read raw data
df = pd.read_sql_query(
    "SELECT * FROM ingestion.sales_details;",
    con=conn
)

print("Raw preview:")
print(df.head())
print("\nShape:", df.shape)

# --------------------------------------------------
# STEP 1 — Fix invalid prices
# Rule: if sls_price is null or <= 0,
# set sls_price = sls_sales / sls_quantity
# --------------------------------------------------
bad_price = df["sls_price"].isnull() | (df["sls_price"] <= 0)

df.loc[bad_price, "sls_price"] = (
    df.loc[bad_price, "sls_sales"] / df.loc[bad_price, "sls_quantity"] #account for negatives here
)

# --------------------------------------------------
# STEP 2 — Recalculate sales for all rows
# Rule: sls_sales = sls_quantity * sls_price
# --------------------------------------------------
df["sls_sales"] = df["sls_quantity"] * df["sls_price"]

# --------------------------------------------------
# STEP 3 — Fix sls_order_dt for orders with multiple items
# Rule:
# If multiple rows have the same sls_ord_num,
# assign the minimum non-zero order date
# that is exactly 8 digits long
# --------------------------------------------------

# Count rows per order
order_counts = df.groupby("sls_ord_num")["sls_ord_num"].transform("count")

# Valid order date = non-zero and 8 digits
valid_order_dt = (
    df["sls_order_dt"].notnull() &
    (df["sls_order_dt"] != 0) &
    (df["sls_order_dt"].astype(str).str.len() == 8)
)

# Helper column containing only valid order dates
df["valid_order_dt"] = df["sls_order_dt"].where(valid_order_dt)

# Minimum valid order date within each order
min_valid_order_dt = df.groupby("sls_ord_num")["valid_order_dt"].transform("max")

# Assign same order date to all rows in multi-line orders
df.loc[order_counts > 1, "sls_order_dt"] = min_valid_order_dt[order_counts > 1]

# --------------------------------------------------
# STEP 4 — Fix single-line orders with invalid order date
# Rule:
# If count(*) = 1 and sls_order_dt is invalid,
# set sls_order_dt = sls_ship_dt
# --------------------------------------------------
invalid_order_dt = (
    df["sls_order_dt"].isnull() |
    (df["sls_order_dt"] == 0) |
    (df["sls_order_dt"].astype(str).str.len() != 8)
)

single_bad_order_dt = (order_counts == 1) & invalid_order_dt

df.loc[single_bad_order_dt, "sls_order_dt"] = df.loc[single_bad_order_dt, "sls_ship_dt"]

# Final fallback: if order date is still missing, use ship date
df["sls_order_dt"] = df["sls_order_dt"].fillna(df["sls_ship_dt"])

# --------------------------------------------------
# STEP 5 — Convert date columns to datetime
# Columns: sls_order_dt, sls_ship_dt, sls_due_dt
# Expected raw format: YYYYMMDD
# --------------------------------------------------
date_cols = ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]

for col in date_cols:
    df[col] = pd.to_datetime(
        df[col].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce"
    )

# --------------------------------------------------
# STEP 6 — Round numeric columns
# --------------------------------------------------
df["sls_price"] = df["sls_price"].round(2)
df["sls_sales"] = df["sls_sales"].round(2)

# --------------------------------------------------
# STEP 7 — Drop helper column
# --------------------------------------------------
df = df.drop(columns=["valid_order_dt"])

print("\nTransformed preview:")
print(df.head(10))

print("\nNull values after transformation:")
print(df[["sls_sales", "sls_quantity", "sls_price", "sls_order_dt", "sls_ship_dt", "sls_due_dt"]].isnull().sum())

# --------------------------------------------------
# STEP 8 — Create schema if not exists
# --------------------------------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

# --------------------------------------------------
# STEP 9 — Load cleaned data into transformation schema
# --------------------------------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="sales_details",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("\nCleaned data loaded into transformation.sales_details")

# 10. Close connections
cur.close()
conn.close()