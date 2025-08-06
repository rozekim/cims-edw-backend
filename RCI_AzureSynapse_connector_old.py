import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Azure Synapse connection details from environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
port = os.getenv('DB_PORT')
driver = os.getenv('DB_DRIVER')

# ------------------------------------------------------------
#  New API Endpoint for Total Structure by Owner (RCI Module)
# ------------------------------------------------------------
def get_operator_structure_data(limit, offset):
    """Fetch distinct operator structure data with pagination from Azure Synapse."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database...")
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        
        connection = pyodbc.connect(conn_str)
        print("✅ Database Connection Successful!")
        
        cursor = connection.cursor()

        # Query to fetch distinct operator structure data with pagination
        query = f"""
        WITH cte AS (
            SELECT 
                OPERATOR AS OWNER,
                COUNT(DISTINCT STRUCTURE_ID) AS [Total RCI],
                FORMAT(ROUND((COUNT(DISTINCT STRUCTURE_ID) * 100.0 / SUM(COUNT(DISTINCT STRUCTURE_ID)) OVER()), 2), 'N2') + '%' AS [Total RCI (%)],
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT STRUCTURE_ID) DESC) AS rn
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES ts
            GROUP BY OPERATOR
        )
        SELECT OWNER, [Total RCI], [Total RCI (%)] 
        FROM cte
        WHERE rn > {offset} AND rn <= {offset} + {limit};
        """
        
        cursor.execute(query)

        # Fetch column names and data
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        # Convert to pandas DataFrame
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ Data retrieval successful!")
        return df

    except Exception as e:
        print(f"\n❌ Database Error: {e}")
        return pd.DataFrame({"error": [str(e)]})


# ------------------------------------------------------------------------------
#  New API Endpoint for Total Structure by Structure Category Data (RCI Module)
# ------------------------------------------------------------------------------
def get_structure_category_data():
    """Fetch total structure count by structure category from Azure Synapse, including percentage."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database...")
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        
        connection = pyodbc.connect(conn_str)
        print("✅ Database Connection Successful!")
        
        cursor = connection.cursor()

        # Query to fetch total structure count by structure category including percentage
        query = """
        WITH StructureCounts AS (
            SELECT 
                STRUCTURE_CATEGORY,
                COUNT(*) AS TOTAL_STRUCTURE
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
            GROUP BY STRUCTURE_CATEGORY
        ), Total AS (
            SELECT SUM(TOTAL_STRUCTURE) AS TOTAL_SUM FROM StructureCounts
        )
        SELECT 
            SC.STRUCTURE_CATEGORY,
            SC.TOTAL_STRUCTURE,
            FORMAT(ROUND((SC.TOTAL_STRUCTURE * 100.0 / T.TOTAL_SUM), 2), 'N2') + '%' AS TOTAL_STRUCTURE_PERCENTAGE
        FROM StructureCounts SC, Total T
        ORDER BY SC.TOTAL_STRUCTURE DESC;
        """
        
        cursor.execute(query)

        # Fetch column names and data
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        # Convert to pandas DataFrame
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ Data retrieval successful!")
        return df

    except Exception as e:
        print(f"\n❌ Database Error: {e}")
        return pd.DataFrame({"error": [str(e)]})
