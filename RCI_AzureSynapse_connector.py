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
#  API Endpoint for Total Structure by Owner (RCI Module)
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
            WHERE STATUS != 'DISCONTINUE'
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
#  API Endpoint for Total Structure by Structure Category Data (RCI Module)
# ------------------------------------------------------------------------------
def get_structure_category_data(operator=None, state=None, district=None, mukim=None, dun=None):
    """Fetch total structure count by structure category from Azure Synapse, including percentage, with filtering."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database for STRUCTURE_CATEGORY data...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Build dynamic WHERE clause for filters
        conditions = ["STATUS != 'DISCONTINUE'"]
        if operator and operator.lower() != "none":
            conditions.append(f"OPERATOR = '{operator}'")
        if state and state.lower() != "none":
            conditions.append(f"STATE = '{state}'")
        if district and district.lower() != "none":
            conditions.append(f"DISTRICT = '{district}'")
        if mukim and mukim.lower() != "none":
            conditions.append(f"MUKIM = '{mukim}'")
        if dun and dun.lower() != "none":
            conditions.append(f"DUN = '{dun}'")

        where_clause = " AND ".join(conditions)

        query = f"""
        WITH StructureCounts AS (
            SELECT 
                STRUCTURE_CATEGORY,
                COUNT(*) AS TOTAL_STRUCTURE
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
            WHERE {where_clause}
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
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ Category structure data retrieved successfully.")
        return df

    except Exception as e:
        print(f"\n❌ Error retrieving structure category data: {e}")
        return pd.DataFrame({"error": [str(e)]})

# ------------------------------------------------------------------------------
#  API Endpoint for Total Owner by Projects Data (RCI Module)
# ------------------------------------------------------------------------------
def get_structure_project_data(operator=None, state=None, district=None, mukim=None, dun=None):
    """Fetch total structure count by PROJECTS from Azure Synapse, with dynamic filtering."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database for project data...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Build dynamic WHERE clause for filters
        conditions = ["STATUS != 'DISCONTINUE'"]
        if operator and operator.lower() != "none":
            conditions.append(f"OPERATOR = '{operator}'")
        if state and state.lower() != "none":
            conditions.append(f"STATE = '{state}'")
        if district and district.lower() != "none":
            conditions.append(f"DISTRICT = '{district}'")
        if mukim and mukim.lower() != "none":
            conditions.append(f"MUKIM = '{mukim}'")
        if dun and dun.lower() != "none":
            conditions.append(f"DUN = '{dun}'")

        where_clause = " AND ".join(conditions)

        query = f"""
        WITH ProjectCounts AS (
            SELECT 
                PROJECTS,
                COUNT(*) AS TOTAL_STRUCTURE
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
            WHERE {where_clause}
            GROUP BY PROJECTS
        ), Total AS (
            SELECT SUM(TOTAL_STRUCTURE) AS TOTAL_SUM FROM ProjectCounts
        )
        SELECT 
            PC.PROJECTS,
            PC.TOTAL_STRUCTURE,
            FORMAT(ROUND((PC.TOTAL_STRUCTURE * 100.0 / T.TOTAL_SUM), 2), 'N2') + '%' AS TOTAL_PERCENTAGE
        FROM ProjectCounts PC, Total T
        ORDER BY PC.TOTAL_STRUCTURE DESC;
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ Project structure data retrieved successfully.")
        return df

    except Exception as e:
        print(f"❌ Error retrieving project structure data: {e}")
        return pd.DataFrame({"error": [str(e)]})


# ------------------------------------------------------------------------------
#  API Endpoint for Total RCI by State Data (RCI Module)
# ------------------------------------------------------------------------------
def get_structure_state_data(operator=None, state=None, district=None, mukim=None, dun=None):
    """Fetch total structure count by STATE from Azure Synapse, with dynamic filtering."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database for state data...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # Build dynamic WHERE clause for filters
        conditions = ["STATUS != 'DISCONTINUE'"]
        if operator and operator.lower() != "none":
            conditions.append(f"OPERATOR = '{operator}'")
        if state and state.lower() != "none":
            conditions.append(f"STATE = '{state}'")
        if district and district.lower() != "none":
            conditions.append(f"DISTRICT = '{district}'")
        if mukim and mukim.lower() != "none":
            conditions.append(f"MUKIM = '{mukim}'")
        if dun and dun.lower() != "none":
            conditions.append(f"DUN = '{dun}'")

        where_clause = " AND ".join(conditions)

        query = f"""
        WITH StateCounts AS (
            SELECT 
                STATE,
                COUNT(*) AS TOTAL_STRUCTURE
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
            WHERE {where_clause}
            GROUP BY STATE
        ), Total AS (
            SELECT SUM(TOTAL_STRUCTURE) AS TOTAL_SUM FROM StateCounts
        )
        SELECT 
            SC.STATE,
            SC.TOTAL_STRUCTURE,
            FORMAT(ROUND((SC.TOTAL_STRUCTURE * 100.0 / T.TOTAL_SUM), 2), 'N2') + '%' AS TOTAL_PERCENTAGE
        FROM StateCounts SC, Total T
        ORDER BY SC.TOTAL_STRUCTURE DESC;
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ State structure data retrieved successfully.")
        return df

    except Exception as e:
        print(f"❌ Error retrieving state structure data: {e}")
        return pd.DataFrame({"error": [str(e)]})


# ------------------------------------------------
#  API Endpoint for Headers Data (RCI Module)
# ------------------------------------------------
def get_structure_summary_data(operator=None, state=None, district=None, mukim=None, dun=None):
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database for summary data...")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        # Build WHERE conditions
        conditions = ["STATUS != 'DISCONTINUE'"]
        if operator:
            conditions.append(f"OPERATOR = '{operator}'")
        if state:
            conditions.append(f"STATE = '{state}'")
        if district:
            conditions.append(f"DISTRICT = '{district}'")
        if mukim:
            conditions.append(f"MUKIM = '{mukim}'")
        if dun:
            conditions.append(f"DUN = '{dun}'")
        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
        SELECT
            COUNT(DISTINCT OPERATOR) AS TOTAL_OPERATOR,
            COUNT(DISTINCT STRUCTURE_CATEGORY) AS TOTAL_STRUCTURE_CATEGORY,
            COUNT(*) AS TOTAL_STRUCTURES,
            COUNT(DISTINCT PROJECTS) AS TOTAL_PROJECTS,
            COUNT(DISTINCT OWNER) AS TOTAL_OWNER
        FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
        {where_clause}
        """

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)
        cursor.close()
        connection.close()
        print("✅ Structure summary data retrieved successfully.")
        return df

    except Exception as e:
        print(f"❌ Error retrieving structure summary data: {e}")
        return pd.DataFrame({"error": [str(e)]})


# ------------------------------------------------------------------------------
#  Function to fetch Tower Structures Data for Map Display (RCI Module)
# ------------------------------------------------------------------------------
def get_tower_structures_data_map(offset=0, limit=10):
    """
    Fetch rows from TOWER_STRUCTURES using ROW_NUMBER() pagination for map display.
    """
    try:
        print("Connecting to Azure Synapse for TOWER_STRUCTURES map data...")

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
            WHERE STATUS != 'DISCONTINUE'
        )
        SELECT STRUCTURE_ID, SERVICE_PROVIDER, OWNER, STRUCTURE_CATEGORY, PROJECTS, 
               STATE, DISTRICT, MUKIM, DUN, PARLIAMENT, LONGITUDE, LATITUDE
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
        print("✅ Map data fetched successfully!")
        return df

    except Exception as e:
        print(f"❌ Error fetching TOWER_STRUCTURES map data: {e}")
        return pd.DataFrame({"error": [str(e)]})
    

# ------------------------------------------------------------------------------
#  Function to Get Filter Options for Tower Structures
# ------------------------------------------------------------------------------
def get_tower_structures_filter_options():
    """Fetch distinct values for filter dropdowns (operator, state, district, mukim, dun)."""
    try:
        print("\n🔍 Debug: Connecting to Azure Synapse Database for filter options...")
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

        # Query to fetch distinct values for each filter option
        queries = {
            "operators": "SELECT DISTINCT OPERATOR FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES WHERE OPERATOR IS NOT NULL AND STATUS != 'DISCONTINUE' ORDER BY OPERATOR",
            "states": "SELECT DISTINCT STATE FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES WHERE STATE IS NOT NULL AND STATUS != 'DISCONTINUE' ORDER BY STATE",
            "districts": "SELECT DISTINCT DISTRICT FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES WHERE DISTRICT IS NOT NULL AND STATUS != 'DISCONTINUE' ORDER BY DISTRICT",
            "mukims": "SELECT DISTINCT MUKIM FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES WHERE MUKIM IS NOT NULL AND STATUS != 'DISCONTINUE' ORDER BY MUKIM",
            "duns": "SELECT DISTINCT DUN FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES WHERE DUN IS NOT NULL AND STATUS != 'DISCONTINUE' ORDER BY DUN",
        }
        
        results = {}
        
        for key, query in queries.items():
            cursor.execute(query)
            # Convert each row's first element to a list
            results[key] = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        print("✅ Filter options retrieved successfully!")
        return results

    except Exception as e:
        print(f"\n❌ Error retrieving filter options: {e}")
        return {"error": str(e)}

# ------------------------------------------------------------------------------
#  **ENHANCED** Function to Get Filtered Tower Structures Data - MAIN FIX
# ------------------------------------------------------------------------------
def get_tower_structures_filtered(operator=None, state=None, district=None, mukim=None, dun=None, offset=0, limit=1000):
    """
    Fetch data from TOWER_STRUCTURES with filter conditions.
    **CRITICAL FIX: Handle empty/None filters properly**
    """
    try:
        print(f"\n🔍 Debug: Connecting to Azure Synapse for filtered TOWER_STRUCTURES data...")
        print(f"📋 Raw filters received: operator='{operator}', state='{state}', district='{district}', mukim='{mukim}', dun='{dun}'")
        
        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()
        print("✅ Connected to Azure Synapse successfully!")

        # **CRITICAL FIX: Check if any filters are actually provided**
        # Clean up filter values and check for meaningful content
        cleaned_filters = {}
        
        def is_meaningful_filter(value):
            """Check if a filter value is meaningful (not empty, null, or 'All...')"""
            if not value:
                return False
            str_value = str(value).strip().lower()
            if str_value in ['', 'none', 'null', 'undefined']:
                return False
            if str_value.startswith('all '):  # 'all operators', 'all states', etc.
                return False
            return True
        
        if is_meaningful_filter(operator):
            cleaned_filters['operator'] = str(operator).strip()
        if is_meaningful_filter(state):
            cleaned_filters['state'] = str(state).strip()
        if is_meaningful_filter(district):
            cleaned_filters['district'] = str(district).strip()
        if is_meaningful_filter(mukim):
            cleaned_filters['mukim'] = str(mukim).strip()
        if is_meaningful_filter(dun):
            cleaned_filters['dun'] = str(dun).strip()
        
        has_filters = len(cleaned_filters) > 0
        print(f"📊 Cleaned filters: {cleaned_filters}")
        print(f"🏷️ Has active filters: {has_filters}")
        
        if not has_filters:
            # **NO FILTERS PROVIDED - Return all data (same as map endpoint)**
            print("📊 No filters provided, returning all data...")
            base_query = f"""
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
                WHERE STATUS != 'DISCONTINUE'
            )
            SELECT STRUCTURE_ID, SERVICE_PROVIDER, OWNER, STRUCTURE_CATEGORY, PROJECTS, 
                   STATE, DISTRICT, MUKIM, DUN, PARLIAMENT, LONGITUDE, LATITUDE
            FROM cte
            WHERE rn > {offset}
              AND rn <= {offset} + {limit}
            """
        else:
            # **FILTERS PROVIDED - Apply them**
            print("🔍 Filters provided, applying conditions...")
            base_query = """
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
                WHERE STATUS != 'DISCONTINUE'
            """
            
            # Add filter conditions using cleaned filters
            conditions = []
            if 'operator' in cleaned_filters:
                conditions.append(f"OPERATOR = '{cleaned_filters['operator']}'")
            if 'state' in cleaned_filters:
                conditions.append(f"STATE = '{cleaned_filters['state']}'")
            if 'district' in cleaned_filters:
                conditions.append(f"DISTRICT = '{cleaned_filters['district']}'")
            if 'mukim' in cleaned_filters:
                conditions.append(f"MUKIM = '{cleaned_filters['mukim']}'")
            if 'dun' in cleaned_filters:
                conditions.append(f"DUN = '{cleaned_filters['dun']}'")
            
            # Add conditions to the query
            if conditions:
                base_query += " AND " + " AND ".join(conditions)
                print(f"📋 Applied conditions: {conditions}")
            
            # Complete the query
            base_query += f"""
            )
            SELECT STRUCTURE_ID, SERVICE_PROVIDER, OWNER, STRUCTURE_CATEGORY, PROJECTS, 
                   STATE, DISTRICT, MUKIM, DUN, PARLIAMENT, LONGITUDE, LATITUDE
            FROM cte
            WHERE rn > {offset}
              AND rn <= {offset} + {limit}
            """

        print("🔍 Executing query...")
        cursor.execute(base_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        connection.close()
        
        result_count = len(df)
        print(f"✅ Filtered data fetched successfully! Returned {result_count} records")
        
        if result_count == 0:
            print("⚠️ Warning: No records found with the applied filters")
        
        return df

    except Exception as e:
        print(f"❌ Error fetching filtered TOWER_STRUCTURES data: {e}")
        return pd.DataFrame({"error": [str(e)]})

# ------------------------------------------------------------------------------
#  Function to Get Dependent Filter Options
# ------------------------------------------------------------------------------
def get_dependent_filter_options(state=None):
    """Fetch dependent filter options based on selected parent filters."""
    try:
        print(f"\n🔍 Debug: Connecting to Azure Synapse Database for dependent filter options (state={state})...")
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

        results = {}
        
        # Get districts for selected state
        if state:
            cursor.execute(f"""
                SELECT DISTINCT DISTRICT 
                FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES 
                WHERE STATE = '{state}' 
                    AND DISTRICT IS NOT NULL
                    AND STATUS != 'DISCONTINUE' 
                ORDER BY DISTRICT
            """)
            results["districts"] = [row[0] for row in cursor.fetchall()]
            
            # Get mukims for selected state
            cursor.execute(f"""
                SELECT DISTINCT MUKIM 
                FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES 
                WHERE STATE = '{state}' 
                    AND MUKIM IS NOT NULL
                    AND STATUS != 'DISCONTINUE'
                ORDER BY MUKIM
            """)
            results["mukims"] = [row[0] for row in cursor.fetchall()]
            
            # Get DUNs for selected state
            cursor.execute(f"""
                SELECT DISTINCT DUN 
                FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES 
                WHERE STATE = '{state}' 
                    AND DUN IS NOT NULL
                    AND STATUS != 'DISCONTINUE'
                ORDER BY DUN
            """)
            results["duns"] = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        print("✅ Dependent filter options retrieved successfully!")
        return results

    except Exception as e:
        print(f"\n❌ Error retrieving dependent filter options: {e}")
        return {"error": str(e)}