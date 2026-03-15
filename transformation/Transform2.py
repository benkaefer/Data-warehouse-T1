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
df = pd.read_sql_query("SELECT * FROM ingestion.prd_info;", con=conn)

print(df.head())

df["cat_id"] = df["prd_key"].str[:5]
df["prd_key"] = df["prd_key"].str[6:]
df["cat_id"] = df["cat_id"].str.replace("-", "_")

print(df.head())




df["prd_cost"] = df["prd_cost"].fillna(0)
df["prd_end_dt"] = df["prd_end_dt"].fillna("NA")
df["prd_line"] = df["prd_line"].fillna("NA")


print(df.head())



# Dates

df["prd_start_dt"] = pd.to_datetime(df["prd_start_dt"], errors="coerce")
df["prd_end_dt"] = pd.to_datetime(df["prd_end_dt"], errors="coerce")

# 4. Sort properly
df = df.sort_values(["prd_key", "prd_id"]).copy()

# 5. Set end date = next start date - 1 day within each prd_key
df["prd_end_dt"] = df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)

# 6. Optional: sort back by prd_id for readability
df = df.sort_values("prd_id").copy()

print(df.head(10))


df["prd_end_dt"] = df["prd_end_dt"].dt.strftime("%Y-%m-%d")
df["prd_end_dt"] = df["prd_end_dt"].fillna("NAT")


# Prd_line change names
df["prd_line"] = df["prd_line"].replace({
    "R": "Road",
    "T": "Touring",
    "M": "Mountain",
    "S": "Sport"
})




cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="prd_info",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into transformation.prd_info")

# 6. Close connections
cur.close()
conn.close()
