from google.cloud import bigquery
from faker import Faker
import random

# Initialize BigQuery client
client = bigquery.Client()

# Define your BigQuery dataset & table
PROJECT_ID = "--your-project-id--"
DATASET_ID = "test_dataset"
TABLE_ID = "dummy_users"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Create dataset if not exists
dataset_ref = client.dataset(DATASET_ID)
try:
    client.get_dataset(dataset_ref)
    print(f"✅ Dataset '{DATASET_ID}' already exists.")
except:
    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset)
    print(f"✅ Created dataset '{DATASET_ID}'.")

# Define Table Schema
schema = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("salary", "FLOAT"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

# Create Table if not exists
try:
    client.get_table(TABLE_REF)
    print(f"✅ Table '{TABLE_ID}' already exists.")
except:
    table = bigquery.Table(TABLE_REF, schema=schema)
    client.create_table(table)
    print(f"✅ Created table '{TABLE_ID}'.")

# Initialize Faker for dummy data
fake = Faker()

# Function to generate dummy data
def generate_dummy_data(batch_size=1000):
    """Generates dummy user data for BigQuery."""
    rows = []
    for i in range(batch_size):
        rows.append({
            "id": random.randint(1000, 999999),
            "name": fake.name(),
            "email": fake.email(),
            "age": random.randint(18, 65),
            "city": fake.city(),
            "salary": round(random.uniform(30000, 120000), 2),
            "created_at": fake.date_time_this_decade().isoformat(),
        })
    return rows

# Insert 50,000 records in batches
BATCH_SIZE = 500
TOTAL_RECORDS = 500

for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
    batch_data = generate_dummy_data(BATCH_SIZE)
    errors = client.insert_rows_json(TABLE_REF, batch_data)
    if errors:
        print(f"❌ Error inserting batch {i}: {errors}")
    else:
        print(f"✅ Successfully inserted batch {i+1} - {i+BATCH_SIZE}")

print(f"🎉 Successfully inserted {TOTAL_RECORDS} records into {TABLE_REF}")
