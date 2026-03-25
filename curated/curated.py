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
customer_crm_df = pd.read_sql_query("SELECT * FROM transformation.cust_info;", con=conn)
customer_erp_df = pd.read_sql_query("SELECT * FROM transformation.cust_az12;", con=conn)
location_erp_df = pd.read_sql_query("SELECT * FROM transformation.loc_a101;", con=conn)

# print(customer_crm_df)
# print(customer_erp_df)
# print(location_erp_df)


df = pd.merge(
    left=customer_crm_df,
    right=customer_erp_df,
    how="left",
    left_on = "cst_key",
    right_on= "cid"
)

print(df)

df = pd.merge(
    left=df,
    right=location_erp_df,
    how="left",
    left_on = "cst_key",
    right_on= "cid",
    suffixes = ("", "_location")
)

print(df)

dim_customers = pd.DataFrame({
    "customer_id": df["cst_id"],
    "customer_nubmer": df["cst_key"],
    "first_name": df["cst_firstname"],
    "last_name": df["cst_lastname"],
    "country": df["cntry"],
    "gender":df["cst_marital_status"],
    "birthdate": df["bdate"],
    "create_date": df["cst_create_date"] 
})

print(dim_customers)

dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
dim_customers.insert(0, "customer_key", dim_customers.index +1)

print(dim_customers)


#product Table

product_crm_df = pd.read_sql_query("SELECT * FROM transformation.prd_info;", con=conn)
category_crm_df = pd.read_sql_query("SELECT * FROM transformation.px_cat_g1v2;", con=conn)


print("Here")
print(product_crm_df)
print(category_crm_df)



df = pd.merge(
    left=product_crm_df,
    right=category_crm_df,
    how="left",
    left_on = "cat_id",
    right_on= "id",
)

print(df)

dim_product = pd.DataFrame({
    "product_number": df["prd_key"],
    "product_name": df["prd_nm"],
    "cost": df["prd_cost"],
    "product_line": df["prd_line"],
    "product_start_date": df["prd_start_dt"],
    "product_end_date": df["prd_end_dt"],
    "category_id": df["cat_id"],
    "category": df["cat"],
    "subcategory": df["subcat"],
    "maintenance": df["maintenance"]
})

print(dim_product)



# dim_product= dim_product.sort_values("").reset_index(drop=True)
dim_product.insert(0, "product_key", dim_product.index +1)

print(dim_product)


# Facts

sales_details = pd.read_sql_query("SELECT * FROM transformation.sales_details;", con=conn)

print(sales_details)

df = pd.merge(
    left=sales_details,
    right=dim_product[["product_key", "product_number"]],
    how="left",
    left_on = "sls_prd_key",
    right_on= "product_number",
)

print(df)

df = pd.merge(
    left=df,
    right=dim_customers[["customer_key", "customer_id"]],
    how="left",
    left_on = "sls_cust_id",
    right_on= "customer_id",
)


print(df)

fact_sales = pd.DataFrame({
    "product_key":df["product_key"],
    "customer_key":df["customer_key"],
    "order_number":df["sls_ord_num"],
    "order_date": df["sls_order_dt"],
    "shipping_date":df["sls_ship_dt"],
    "due_date":df["sls_due_dt"],
    "sales":df["sls_sales"],
    "quantity":df["sls_quantity"],
    "price":df["sls_price"]
})

fact_sales.insert(0, "order_key", fact_sales.index +1)

print(fact_sales)


cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()





cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:Ben080502@127.0.0.1:5433/dataengineering_project"
)

dim_customers.to_sql(
    name="dim_customers",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

dim_product.to_sql(
    name="dim_product",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

fact_sales.to_sql(
    name="fact_sales",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("Tables loaded into curated schema")

cur.close()
conn.close()


