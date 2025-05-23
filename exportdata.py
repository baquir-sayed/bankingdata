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


conn = psycopg2.connect(
    host = db_host,
    port = db_port,
    database = db_name,
    user = db_user,
    password = db_password
)

# Creates a table in the postgresql database if it doesn't exist already. The f-string is used to basically give us the table name and write in a SQL command as a string.
# The table has the same column names as in the CSV file. Can be changed if necessary.
# cur is known as a "cursor object" and is used to execute SQL commands. Mind that cur has conn in it, which is the connection object we've made.

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

# Buffer is an object that loads us the CSV file into RAM. Not the most efficient way, ideally we would use the dataframe and load it into chunks for larger datasets.
# df.to_csv(buffer, index=False, header=False) basically converts it into a csv format without the index and header. The buffer.seek(0) makes sure that the buffer starts
# from the start of the buffer file. The cur.copy_from(...) copies the data from the buffer into the database table, sep="." means the delimiter is a comma.
# conn.commit() commits the changes to the database, we need to do this as it won't save otherwise. Then we close the cursor and the connection.


buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_from(buffer, db_table, sep=",")
conn.commit()

cur.close()
conn.close()
