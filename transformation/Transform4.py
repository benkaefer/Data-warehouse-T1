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
    "SELECT * FROM ingestion.cust_az12;",
    con=conn
)

print("Raw preview:")
print(df.head())
print("\nShape:", df.shape)

# --------------------------------------------------
# STEP 1 — Fix customer ID
# Rule:
# If cid has length 13, remove first 3 letters "NAS"
# --------------------------------------------------
df["cid"] = df["cid"].astype(str).str.strip()

df.loc[df["cid"].str.len() == 13, "cid"] = df.loc[
    df["cid"].str.len() == 13, "cid"
].str[3:]

# --------------------------------------------------
# STEP 2 — Fix gender values
# Rule:
# nulls / empty strings / blanks -> "NA"
# F / female -> "Female"
# M / male -> "Male"
# --------------------------------------------------
df["gen"] = df["gen"].fillna("").astype(str).str.strip().str.lower()

df["gen"] = df["gen"].replace({
    "": "NA",
    "f": "Female",
    "female": "Female",
    "m": "Male",
    "male": "Male"
})

# if anything unexpected remains, also set to NA
df.loc[~df["gen"].isin(["Female", "Male", "NA"]), "gen"] = "NA"

# --------------------------------------------------
# STEP 3 — Fix birth date
# Rule:
# convert to datetime
# if bdate > current date -> NaT
# --------------------------------------------------
df["bdate"] = pd.to_datetime(df["bdate"], errors="coerce")

today = pd.Timestamp.today().normalize()
df.loc[df["bdate"] > today, "bdate"] = None

# # --------------------------------------------------
# # STEP 4 
# # --------------------------------------------------


print("\nTransformed preview:")
print(df.head(10))

print("\nNull values after transformation:")
print(df.isnull().sum())

# --------------------------------------------------
# STEP 5 — Create schema if not exists
# --------------------------------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

# --------------------------------------------------
# STEP 6 — Load cleaned data into transformation schema
# --------------------------------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="cust_az12",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("\nCleaned data loaded into transformation.cust_az12")

# 7. Close connections
cur.close()
conn.close()