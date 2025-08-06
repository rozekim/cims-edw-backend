import cx_Oracle
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from environment variables
username = os.getenv('Username_Oracle')
password = os.getenv('Password_Oracle')

cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")

def get_fos_port_data(limit=5, offset=0):
    """Fetch paginated data from the Oracle database and return as JSON."""
    try:
        print("\n🔍 Debug: Connecting to Database...")
        dsn = cx_Oracle.makedsn(
            host="192.168.11.198",
            port=1521,
            service_name="mcmccims"
        )
        
        connection = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        print("✅ Database Connection Successful!")

        cursor = connection.cursor()
        
        # Paginate query using `OFFSET` and `FETCH FIRST N ROWS ONLY`
        query = f"""
        SELECT * FROM EDW.V_FOS_PORT
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """
        cursor.execute(query)

        # Get column names and data
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()

        # Convert to list of dictionaries
        result = [dict(zip(columns, row)) for row in data]

        cursor.close()
        connection.close()
        return result

    except cx_Oracle.Error as error:
        print(f"\n❌ Database Error: {error}")
        return {"error": f"Database error: {error}"}



