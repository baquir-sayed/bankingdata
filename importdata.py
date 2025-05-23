import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv

# Function to load data from a PostgreSQL database and return it as a pandas DataFrame. Use it to import in a Jupyter notebook.

def get_data():
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_table = os.getenv("DB_TABLE")

    conn = psycopg2.connect(
        host = db_host,
        port = db_port,
        database = db_name,
        user = db_user,
        password = db_password
    )

    # Using f-string here to execute a SQL command to show us a subset of the table.


    cur = conn.cursor()
    cur.execute(f"""
                SELECT * FROM {db_table}
                """)

    # The fetchall() gets all the data from the cursor object above. The "columns = [desc[0] for desc in cur.description]" is used to get the names of the columns from the
    # cursor object. It is then printed as a dataframe.

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    print(df)


    cur.close()
    conn.close()
    return df