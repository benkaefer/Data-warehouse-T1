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
df = pd.read_sql_query("SELECT * FROM ingestion.cust_info;", con=conn)

# 3. Transform data

# Remove NULL cst_id
df = df[df["cst_id"].notna()]

# Remove duplicates
df = df.drop_duplicates(subset="cst_id", keep="last")

# Save original firstname for comparison
original = df["cst_firstname"].copy()

# Strip whitespace from all text columns
str_cols = df.select_dtypes(include=["object", "string"]).columns
for col in str_cols:
    df[col] = df[col].astype("string").str.strip()

# Check firstname changes
changed_rows = df[original.astype("string").str.strip() != df["cst_firstname"]]
print("Changed firstname rows:")
print(changed_rows[["cst_id", "cst_firstname"]])

# Convert empty strings to missing values
df["cst_marital_status"] = df["cst_marital_status"].replace("", pd.NA)
df["cst_gndr"] = df["cst_gndr"].replace("", pd.NA)

# Replace codes
df["cst_marital_status"] = df["cst_marital_status"].replace({
    "S": "Single",
    "M": "Married"
})

df["cst_gndr"] = df["cst_gndr"].replace({
    "M": "Male",
    "F": "Female"
})

# Fill missing values
df["cst_marital_status"] = df["cst_marital_status"].fillna("NA")
df["cst_gndr"] = df["cst_gndr"].fillna("NA")

# Debug check
print(df["cst_gndr"].value_counts(dropna=False))
print(df["cst_marital_status"].value_counts(dropna=False))
print(df["cst_gndr"].unique())
print(df["cst_marital_status"].unique())

# 4. Create schema
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

# 5. Load cleaned data
engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="cust_info",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.cust_info")

# 6. Close connections
cur.close()
conn.close()