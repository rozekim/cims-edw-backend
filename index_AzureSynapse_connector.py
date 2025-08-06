import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
port = os.getenv('DB_PORT')
driver = os.getenv('DB_DRIVER')

def get_mb_network_data(offset=0, limit=10):
    """
    Fetch unique rows from MB_NETWORK using ROW_NUMBER() pagination,
    ensuring that only one record per MB_NETWORK_ID is retrieved.
    """
    try:
        print("Connecting to Azure Synapse...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        # query = f"""
        # WITH cte AS (
        #     SELECT 
        #         MB_NETWORK_ID,
        #         SERVICE_PROVIDER,
        #         HOST,
        #         SHARER,
        #         BACKHAUL,
        #         NETWORK_TYPE,
        #         X AS LONGITUDE,
        #         Y AS LATITUDE,
        #         STATE,
        #         DISTRICT,
        #         MUKIM,
        #         DUN,
        #         PARLIAMENT,
        #         ROW_NUMBER() OVER (ORDER BY MB_NETWORK_ID) AS rn
        #     FROM [Dedicated SQL Pool].cims_geo.MB_NETWORK
        # )
        # SELECT *
        # FROM cte
        # WHERE rn > {offset}
        # AND rn <= {offset} + {limit}
        # """

        # # SQL query to remove duplicate MB_NETWORK_ID and paginate results
        query = f"""
        WITH cte AS (
            SELECT 
                MB_NETWORK_ID,
                SERVICE_PROVIDER,
                HOST,
                SHARER,
                BACKHAUL,
                NETWORK_TYPE,
                X AS LONGITUDE,
                Y AS LATITUDE,
                STATE,
                DISTRICT,
                MUKIM,
                DUN,
                PARLIAMENT,
                ROW_NUMBER() OVER (PARTITION BY MB_NETWORK_ID ORDER BY MB_NETWORK_ID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.MB_NETWORK
        ),
        filtered AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY MB_NETWORK_ID) AS row_num
            FROM cte
            WHERE rn = 1
        )
        SELECT * 
        FROM filtered
        WHERE row_num > {offset} AND row_num <= {offset} + {limit};
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ Data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching MB_NETWORK data: {e}")
        return pd.DataFrame({"error": [str(e)]})
    

def get_tower_structures_data(offset=0, limit=10):
    """
    Fetch rows from TOWER_STRUCTURES using ROW_NUMBER() pagination.
    """
    try:
        print("Connecting to Azure Synapse for TOWER_STRUCTURES...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        query = f"""
        WITH cte AS (
            SELECT 
                STRUCTURE_ID,
                OPERATOR AS SERVICE_PROVIDER,
                OWNER,
                STRUCTURE_CATEGORY,
                PROJECTS,
                STATE,
                DISTRICT,
                MUKIM,
                DUN,
                PARLIAMENT,
                X AS LONGITUDE,
                Y AS LATITUDE,
                ROW_NUMBER() OVER (ORDER BY STRUCTURE_ID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
        )
        SELECT *
        FROM cte
        WHERE rn > {offset}
          AND rn <= {offset} + {limit}
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ Data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching TOWER_STRUCTURES data: {e}")
        return pd.DataFrame({"error": [str(e)]})


def get_fiber_optic_site_data(offset=0, limit=10):
    """
    Fetch rows from FIBER_OPTIC_SITE using ROW_NUMBER() pagination.
    Only includes the parameters shown in your image:
      REFID, SERVICE_PROVIDER, CATEGORY, PROJECT, 
      STRUCTURE_TYPE_CODE (as STRUCTURE_TYPE), DISTRICT, MUKIM, DUN, 
      PARLIAMENT, X as LONGITUDE, Y as LATITUDE.
    """
    try:
        print("Connecting to Azure Synapse for FIBER_OPTIC_SITE...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        query = f"""
        WITH cte AS (
            SELECT 
                REFID,
                SERVICE_PROVIDER,
                CATEGORY,
                PROJECT,
                STRUCTURE_TYPE_CODE AS STRUCTURE_TYPE,
                STATE,
                DISTRICT,
                MUKIM,
                DUN,
                PARLIAMENT,
                X AS LONGITUDE,
                Y AS LATITUDE,
                ROW_NUMBER() OVER (ORDER BY ID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.FIBER_OPTIC_SITE
        )
        SELECT *
        FROM cte
        WHERE rn > {offset}
          AND rn <= {offset} + {limit}
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ FIBER_OPTIC_SITE data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching FIBER_OPTIC_SITE data: {e}")
        return pd.DataFrame({"error": [str(e)]})
    

def get_pudo_data(offset=0, limit=10):
    """
    Fetch rows from PUDO using ROW_NUMBER() pagination,
    selecting only the columns from the screenshot:
      REFID, SERVICE_PROVIDER, PUDO_SERVICE_TYPE, BUILDING_TYPE,
      TYPE_INFRASTRUCTURE, DISTRICT, MUKIM, DUN, PARLIAMENT,
      LONGITUDE, LATITUDE.
    
    Note: Because the table actually has BUILDING_CATEGORY (not BUILDING_TYPE),
    we select BUILDING_CATEGORY as BUILDING_TYPE to match the screenshot.
    """
    try:
        print("Connecting to Azure Synapse for PUDO...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        # Use ROW_NUMBER() for pagination
        query = f"""
        WITH cte AS (
            SELECT
                REFID,
                SERVICE_PROVIDER,
                PUDO_SERVICE_TYPE,
                -- The table has BUILDING_CATEGORY, so alias it to BUILDING_TYPE:
                BUILDING_CATEGORY AS BUILDING_TYPE,
                TYPE_INFRASTRUCTURE,
                STATE,
                DISTRICT,
                MUKIM,
                DUN,
                PARLIAMENT,
                X AS LONGITUDE,
                Y AS LATITUDE,
                ROW_NUMBER() OVER (ORDER BY REFID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.PUDO
        )
        SELECT *
        FROM cte
        WHERE rn > {offset}
          AND rn <= {offset} + {limit}
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ PUDO data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching PUDO data: {e}")
        return pd.DataFrame({"error": [str(e)]})
    

def get_pedi_data(offset=0, limit=10):
    """
    Fetch rows from PEDI using ROW_NUMBER() pagination.
    Selecting only:
      MASKED_ID,
      SITE_NAME,
      STATE,
      X as LONGITUDE,
      Y as LATITUDE
    """
    try:
        print("Connecting to Azure Synapse for PEDI...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        query = f"""
        WITH cte AS (
            SELECT
                MASKED_ID,
                SITE_NAME,
                STATE,
                X AS LONGITUDE,
                Y AS LATITUDE,
                ROW_NUMBER() OVER (ORDER BY MASKED_ID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.PEDI
        )
        SELECT *
        FROM cte
        WHERE rn > {offset}
          AND rn <= {offset} + {limit}
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ PEDI data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching PEDI data: {e}")
        return pd.DataFrame({"error": [str(e)]})


def get_mb_moran_mocn_data(offset=0, limit=10):
    """
    Fetch rows from MB_MORAN_MOCN_FULL using ROW_NUMBER() pagination,
    selecting only:
      MB_NETWORK_ID, HOST, SHARER, ATN_AZIMUTH, DATE_CREATED, UPDATED.
      
    The date fields are converted to VARCHAR(10) (format 'yyyy-mm-dd')
    directly in the SQL query.
    """
    try:
        print("Connecting to Azure Synapse for MB_MORAN_MOCN_FULL...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        query = f"""
        WITH cte AS (
            SELECT 
                MB_NETWORK_ID,
                HOST,
                SHARER,
                ATN_AZIMUTH,
                ROW_NUMBER() OVER (ORDER BY MB_NETWORK_ID) AS rn
            FROM [Dedicated SQL Pool].cims_geo.MB_MORAN_MOCN_FULL
        )
        SELECT * FROM cte WHERE rn > {offset} AND rn <= {offset} + {limit};
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()
        print("✅ MB_MORAN_MOCN_FULL data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching MB_MORAN_MOCN data: {e}")
        return pd.DataFrame({"error": [str(e)]})


# Add this function to index_AzureSynapse_connector.py
def get_mb_network_count():
    """Get total count of MB_NETWORK records."""
    try:
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        query = """
        SELECT COUNT(*) as total 
        FROM [Dedicated SQL Pool].cims_geo.MB_NETWORK
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result[0] if result else 0
    except Exception as e:
        print(f"Error getting count: {e}")
        return 0

if __name__ == "__main__":
    df_test = get_mb_network_data(offset=0, limit=10)
    print("Sample MB_NETWORK data:\n", df_test.head())
