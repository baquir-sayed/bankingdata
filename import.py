from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
import io

# This loads a bunch of variables from the .env file. load_dotenv() basically allows us to get all variables from that file.

load_dotenv()
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
csv_path =  os.getenv("CSV_PATH")
db_table = os.getenv("DB_TABLE")



df = pd.read_csv(csv_path)

# Takes details from the .env file. Basically, we're using our connection details to then auth and make changes to the database.
# conn connects to an existing database, which has been created already.
# cur is known as a "cursor object" and is used to execute SQL commands. Mind that cur has conn in it, which is the connection object we've made.

conn = psycopg2.connect(
    host = db_host,
    port = db_port,
    database = db_name,
    user = db_user,
    password = db_password
)
db_table = os.getenv("DB_TABLE")

cur = conn.cursor()
cur.execute(f"""
CREATE TABLE IF NOT EXISTS {db_table} (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT,
    transaction_amount NUMERIC,
    transaction_date TIMESTAMP,
    transaction_type TEXT,
    location TEXT,
    device_id TEXT,
    ip_address TEXT,
    merchant_id TEXT,
    channel TEXT,
    customer_age INT,
    customer_occupation TEXT,
    transaction_duration INT,
    login_attempts INT,
    account_balance NUMERIC,
    previous_transaction_date TIMESTAMP);
    """)

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_from(buffer, db_table, sep=",")
conn.commit()

cur.close()
conn.close()
