import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    dbname="dataengineering_project",
    user="postgres",
    password="Ben080502"
)
cur = conn.cursor()

# ----- SOURCE FILES -----
FILES = {
    "cust_info": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_crm/cust_info.csv",
    "prd_info": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_crm/prd_info.csv",
    "sales_details": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_crm/sales_details.csv",
    "cust_az12": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_erp/CUST_AZ12.csv",
    "loc_a101": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_erp/LOC_A101.csv",
    "px_cat_g1v2": "/Users/benkaefer/Desktop/2026 UM/DataEngineering/datasets/source_erp/PX_CAT_G1V2.csv",
}

# ----- LOAD EACH TABLE -----
for table, path in FILES.items():
    print(f"\n➡️ Loading {table} from {path}")

    # 1) Clear table
    cur.execute(f"TRUNCATE TABLE ingestion.{table};")

    # 2) Bulk load
    with open(path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            f"""
            COPY ingestion.{table}
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
            """,
            f
        )

    print(f"✅ Loaded {table}")

conn.commit()
cur.close()
conn.close()

print("\n🎉 All tables loaded successfully.")