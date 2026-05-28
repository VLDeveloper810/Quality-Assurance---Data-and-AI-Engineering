import csv
import random
import os
import io
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Configuration Flag
GENERATE_CORRUPTED = False  

COMPANIES = [
    ("Global Logistics", "123 Logistics Way, New York, NY"),
    ("TechCorp", "456 Innovation Dr, San Jose, CA"),
    ("Apex Industries", "789 Industrial Blvd, Chicago, IL"),
    ("Vertex Solutions", "101 Summit Rd, Austin, TX"),
    ("Nova Energy", "202 Solar Ln, Houston, TX"),
    ("Quantum Retail", "303 Market St, Seattle, WA"),
    ("Horizon Health", "404 Medical Ctr, Boston, MA"),
    ("Vanguard Aerospace", "505 Runway Ave, Los Angeles, CA"),
    ("Sterling Foods", "606 Grocery Rd, Atlanta, GA"),
    ("Matrix Financial", "707 Wall St, Charlotte, NC")
]

SUFFIXES = ["Inc.", "LLC", "Corp.", "Incorporated", "Group", "Co.", "Ltd."]
CITIES = ["New York", "San Jose", "Chicago", "Austin", "Houston", "Seattle", "Boston", "Los Angeles", "Atlanta", "Charlotte"]
STATES = ["NY", "CA", "IL", "TX", "TX", "WA", "MA", "CA", "GA", "NC"]
SUPPLIERS_POOL = ["Alpha Parts", "Beta Log", "Gamma Mfg", "Delta Tech", "Omega Supply", "Zeta Corp", "Sigma Materials"]
CUSTOMERS_POOL = ["Target Corp", "Walmart Inc", "US Government", "Apple", "Amazon", "General Electric", "Ford"]

# Load environment variables cleanly
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

def generate_and_upload_datasets():
    print(f"--> Generating mockup datasets inside RAM memory structures (Mode: {'CORRUPTED' if GENERATE_CORRUPTED else 'CLEAN'})...")
    
    AWS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET = os.getenv('AWS_SECRET_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'ap-south-1')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    if not all([AWS_KEY, AWS_SECRET, BUCKET_NAME]):
        print("\n[CRITICAL ERROR]: S3 environment variables missing in .env. Execution halted.")
        return

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

    num_records = 1050

    # DATASET 1: SUPPLY CHAIN DATA
    s1_buffer = io.StringIO()
    writer1 = csv.writer(s1_buffer)
    writer1.writerow(['corporate_name_S1', 'address', 'activity_places', 'top_suppliers'])
    
    for i in range(num_records):
        base_idx = i % len(COMPANIES)
        base_name, base_address = COMPANIES[base_idx]
        suffix = random.choice(SUFFIXES)
        corp_name = f"{base_name} {suffix}" if random.random() > 0.1 else f"{base_name}_{i}"
        
        if GENERATE_CORRUPTED and random.random() < 0.05:
            corp_name = ""
            
        address = base_address if random.random() > 0.2 else f"{random.randint(100, 999)} Main St, {CITIES[base_idx]}, {STATES[base_idx]}"
        activity_places = f"{CITIES[base_idx]}; {random.choice(CITIES)}"
        top_suppliers = ";".join(random.sample(SUPPLIERS_POOL, k=random.randint(1, 4)))
        writer1.writerow([corp_name, address, activity_places, top_suppliers])

    # DATASET 2: FINANCIAL DATA (IN-MEMORY BUFFER)
    s2_buffer = io.StringIO()
    writer2 = csv.writer(s2_buffer)
    writer2.writerow(['corporate_name_S2', 'main_customers', 'revenue', 'profit'])
    
    for i in range(num_records):
        base_idx = i % len(COMPANIES)
        base_name, _ = COMPANIES[base_idx]
        alt_suffix = random.choice(SUFFIXES)
        corp_name = f"{base_name} {alt_suffix}" if random.random() > 0.1 else f"{base_name} International"
        main_customers = ";".join(random.sample(CUSTOMERS_POOL, k=random.randint(1, 3)))
        
        if GENERATE_CORRUPTED and random.random() < 0.02:
            revenue = random.randint(-10000, 5000)
        else:
            revenue = random.randint(500000, 100000000)
            
        profit = int(revenue * random.uniform(0.05, 0.25))
        writer2.writerow([corp_name, main_customers, revenue, profit])

    print("--> [Boto3] Streaming memory streams straight into S3 Cloud Storage...")
    try:
        # Stream buffer directly without saving anything to the drive
        s3_client.put_object(Bucket=BUCKET_NAME, Key="supply_chain/source1_supply_chain.csv", Body=s1_buffer.getvalue())
        print("    [STREAMED] -> supply_chain/source1_supply_chain.csv")

        s3_client.put_object(Bucket=BUCKET_NAME, Key="financial/source2_financial.csv", Body=s2_buffer.getvalue())
        print("    [STREAMED] -> financial/source2_financial.csv")
        
        print("🎉 Cloud dataset synchronization complete! Virtual allocations freed.")
    except Exception as e:
        print(f"[AWS S3 STREAM ERROR]: Failed to push memory streams directly to cloud objects.\nDetails: {e}")

if __name__ == "__main__":
    generate_and_upload_datasets()