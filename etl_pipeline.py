import importlib
import sys
import os
import time
from datetime import datetime
import schedule

# Add subdirectories to Python path so importlib can find all modules
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "Data-warehouse-T1", "ingestion"))
sys.path.insert(0, os.path.join(BASE_DIR, "Data-warehouse-T1", "transformation"))
sys.path.insert(0, os.path.join(BASE_DIR, "Data-warehouse-T1", "curated"))

# Helper
def run_module(module_name, label):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running {label}...")
    module = importlib.import_module(module_name)
    importlib.reload(module)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finished {label} ✅")

# Setup jobs
def run_creation():
    run_module("creation", "creation - schema + tables")

def run_load():
    run_module("load", "load - CSV files into ingestion")

# Transformation jobs
def run_transform1():
    run_module("Transform", "Transform - cust_info")

def run_transform2():
    run_module("Transform2", "Transform2 - prd_info")

def run_transform3():
    run_module("Transform3", "Transform3 - sales_details")

def run_transform4():
    run_module("Transform4", "Transform4 - cust_az12")

def run_transform5():
    run_module("Transform5", "Transform5 - loc_a101")

def run_transform6():
    run_module("Transform6", "Transform6 - px_cat_g1v2")

# Curated job
def run_curated():
    run_module("curated", "curated - dim_customers, dim_product, fact_sales")

# Full pipeline
def run_full_pipeline():
    print("\n========================================")
    print(f"PIPELINE STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================")

    run_creation()
    run_load()

    run_transform1()
    run_transform2()
    run_transform3()
    run_transform4()
    run_transform5()
    run_transform6()

    run_curated()

    print("\n========================================")
    print(f"PIPELINE FINISHED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================\n")

if __name__ == "__main__":
    # Run once immediately
    run_full_pipeline()

    # Schedule recurring runs
    schedule.every().day.at("01:00").do(run_load)
    schedule.every().day.at("01:10").do(run_transform1)
    schedule.every().day.at("01:12").do(run_transform2)
    schedule.every().day.at("01:14").do(run_transform3)
    schedule.every().day.at("01:16").do(run_transform4)
    schedule.every().day.at("01:18").do(run_transform5)
    schedule.every().day.at("01:20").do(run_transform6)
    schedule.every().day.at("01:30").do(run_curated)

    print("Scheduler started...")
    while True:
        schedule.run_pending()
        time.sleep(5)