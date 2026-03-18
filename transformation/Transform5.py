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
    "SELECT * FROM ingestion.loc_a101;",
    con=conn
)

print("Raw preview:")
print(df.head())
print("\nShape:", df.shape)

# --------------------------------------------------
# STEP 1 — Fix ID column
# Rule:
# remove leading/trailing spaces
# remove "-"
# Example: AW-00011000 -> AW00011000
# --------------------------------------------------
df["cid"] = df["cid"].astype(str).str.strip().str.replace("-", "", regex=False)

# --------------------------------------------------
# STEP 2 — Fix country column
# Rule:
# DE -> Germany
# US / Us / USA -> United States
# null / "" / blanks -> NA
# --------------------------------------------------
df["cntry"] = df["cntry"].fillna("").astype(str).str.strip()

df.loc[df["cntry"] == "", "cntry"] = "NA"
df.loc[df["cntry"].str.upper() == "DE", "cntry"] = "Germany"
df.loc[df["cntry"].str.upper().isin(["US", "USA"]), "cntry"] = "United States"

print("\nTransformed preview:")
print(df.head(10))

print("\nDistinct countries after transformation:")
print(df["cntry"].value_counts(dropna=False))

# --------------------------------------------------
# STEP 3 — Create schema if not exists
# --------------------------------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

# --------------------------------------------------
# STEP 4 — Load cleaned data into transformation schema
# --------------------------------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="loc_a101",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("\nCleaned data loaded into transformation.loc_a101")

# 5. Close connections
cur.close()
conn.close()