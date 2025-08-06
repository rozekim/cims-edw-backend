import cx_Oracle
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
username = os.getenv('DB_USERNAME')  # Ensure your .env file has DB_USERNAME
password = os.getenv('DB_PASSWORD')  # Ensure your .env file has DB_PASSWORD
dsn = cx_Oracle.makedsn("192.168.11.198", 1521, service_name="mcmccims")

# Establish connection
try:
    connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
    print("✅ Connected to Oracle Database!")

    # Create a cursor
    cursor = connection.cursor()

    # Example: Querying a table
    # query = "SELECT * FROM your_table_name"  # Replace with your actual table name
    query = "SELECT * FROM EDW.V_FOS_PORT"  # Replace with your actual table name
    cursor.execute(query)

    # Fetch column names
    columns = [col[0] for col in cursor.description]

    # Fetch data
    rows = cursor.fetchall()

    # Convert to Pandas DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Close cursor and connection
    cursor.close()
    connection.close()
    print("🔒 Connection closed.")

except cx_Oracle.DatabaseError as e:
    print("❌ Error connecting to Oracle Database:", e)


print(df)
