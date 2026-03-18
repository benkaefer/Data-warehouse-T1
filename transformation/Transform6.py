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

# 2. Load data from ingestion
df = pd.read_sql_query(
    "SELECT * FROM ingestion.px_cat_g1v2;",
    con=conn
)

print("Raw preview:")
print(df.head())
print("\nShape:", df.shape)

# --------------------------------------------------
# STEP 3 — Create transformation schema if not exists
# --------------------------------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
conn.commit()

# --------------------------------------------------
# STEP 4 — Load into transformation schema
# --------------------------------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

df.to_sql(
    name="px_cat_g1v2",
    con=engine,
    schema="transformation",
    if_exists="replace",
    index=False
)

print("\nTable loaded into transformation.px_cat_g1v2")

# 5. Close connections
cur.close()
conn.close()