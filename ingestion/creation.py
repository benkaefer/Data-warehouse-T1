### CREATE SCHEMA

import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,             
    dbname="dataengineering_project",  # postgre before creating the database
    user="postgres",
    password="Ben080502"
)
conn.autocommit = True
#cur = conn.cursor()

# db_name = "dataengineering_project"

# cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (db_name,))
# if cur.fetchone() is None:
#     cur.execute(f'CREATE DATABASE "{db_name}";')
#     print(f"✅ Created database: {db_name}")
# else:
#     print(f"ℹ️ Database already exists: {db_name}")

cur = conn.cursor()

cur.execute("""
CREATE SCHEMA IF NOT EXISTS ingestion;
""")

conn.commit()
cur.close()

print("✅ Schema 'ingestion' created (or already exists).")

cur.close()
conn.close()









import psycopg2

DDL = """
CREATE SCHEMA IF NOT EXISTS ingestion;

DROP TABLE IF EXISTS ingestion.cust_info;
CREATE TABLE ingestion.cust_info (
    cst_id              INTEGER        ,
    cst_key             VARCHAR(20)   ,
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  CHAR(1),
    cst_gndr            CHAR(1),
    cst_create_date     varchar(20)

);

DROP TABLE IF EXISTS ingestion.prd_info;
CREATE TABLE ingestion.prd_info (
    prd_id        INTEGER        ,
    prd_key       VARCHAR(50)    ,
    prd_nm        VARCHAR(100),
    prd_cost      NUMERIC(10,2),
    prd_line      CHAR(1),
    prd_start_dt  varchar(20),
    prd_end_dt    varchar(20)

);

DROP TABLE IF EXISTS ingestion.sales_details;
CREATE TABLE ingestion.sales_details (
    sls_ord_num   VARCHAR(20)    ,
    sls_prd_key   VARCHAR(50)    ,
    sls_cust_id   INTEGER        ,
    sls_order_dt  INTEGER,
    sls_ship_dt   INTEGER,
    sls_due_dt    INTEGER,
    sls_sales     NUMERIC(12,2),
    sls_quantity  INTEGER,
    sls_price     NUMERIC(12,2)
);

DROP TABLE IF EXISTS ingestion.cust_az12;
CREATE TABLE ingestion.cust_az12 (
    cid     VARCHAR(20)    ,
    bdate   varchar(20),
    gen     VARCHAR(10)

);

DROP TABLE IF EXISTS ingestion.loc_a101;
CREATE TABLE ingestion.loc_a101 (
    cid    VARCHAR(20)   ,
    cntry  VARCHAR(50)

);

DROP TABLE IF EXISTS ingestion.px_cat_g1v2;
CREATE TABLE ingestion.px_cat_g1v2 (
    id           VARCHAR(10)    ,
    cat          VARCHAR(50)    ,
    subcat       VARCHAR(100)   ,
    maintenance  BOOLEAN

);
"""

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    dbname="dataengineering_project",
    user="postgres",
    password="Ben080502"
)
conn.autocommit = True
cur = conn.cursor()



# Split on semicolons and run each statement
statements = [s.strip() for s in DDL.split(";") if s.strip()]

for i, stmt in enumerate(statements, start=1):
    try:
        cur.execute(stmt)
        print(f"✅ ({i}/{len(statements)}) Executed: {stmt.splitlines()[0][:80]}")
    except Exception as e:
        print(f"\n❌ Failed on statement #{i}:\n{stmt}\n")
        raise

cur.close()
conn.close()

print("\n🎉 All ingestion DDL executed successfully.")