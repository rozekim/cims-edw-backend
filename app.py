from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import pandas as pd
import supabase
from dotenv import load_dotenv
load_dotenv()

# Import the MB_NETWORK, TOWER_STRUCTURES, and FIBER_OPTIC_SITE data functions from your connector
from index_AzureSynapse_connector import (
    get_mb_network_data,
    get_tower_structures_data,
    get_fiber_optic_site_data,  # new function
    get_pudo_data,
    get_pedi_data,
    get_mb_moran_mocn_data
)

# This is your Azure connector for operator structure
# from RCI_Azure_connector import get_operator_structure_data
from RCI_AzureSynapse_connector import (
    get_operator_structure_data,
    get_structure_category_data,
    get_structure_project_data,
    get_structure_state_data,
    get_structure_summary_data,
    get_tower_structures_data_map,
    get_tower_structures_filtered          # ADD THIS - CRITICAL MISSING IMPORT
)

# Load environment variables
load_dotenv()

def clean_dataframe(df):
    """Replace NaN/NaT with None for JSON serialization."""
    if df is None or df.empty:
        return pd.DataFrame()
    return df.astype(object).where(pd.notnull(df), None)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are missing. Check your .env file.")

# Initialize Supabase client
supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

# Flask app initialization
app = Flask(__name__)
CORS(app)

# ----------------------------------------
#  Home route
# ----------------------------------------
@app.route("/")
def home():
    return jsonify({"message": "Welcome to MCMC API!"})

# ----------------------------------------
#  User Registration (Without Password Hashing)
# ----------------------------------------
@app.route("/api/register", methods=["POST"])
def register():
    """Register a new user with plain password."""
    data = request.json
    email = data.get("email")
    password = data.get("password")
    role = data.get("role", "user")  # Default role is "user"
    operator = data.get("operator")  # For telco users

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    try:
        user_data = {
            "EMAIL": email,
            "PASSWORD": password,
            "ROLE": role
        }
        
        # Add operator field only if provided and role is telco
        if role == "telco" and operator:
            user_data["OPERATOR"] = operator
            
        response = supabase_client.table("Login_EDW").insert(user_data).execute()

        return jsonify({"message": "User registered successfully", "email": email})
    except Exception as e:
        return jsonify({"error": f"Registration failed: {str(e)}"}), 500

# ----------------------------------------
#  User Login (Fixed to handle role and operator validation)
# ----------------------------------------
@app.route("/api/login", methods=["POST"])
def login():
    """User login authentication with direct password comparison and role/operator validation."""
    data = request.json
    email = data.get("email")
    password = data.get("password")
    selected_role = data.get("role")
    selected_operator = data.get("operator")

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    try:
        response = supabase_client.table("Login_EDW").select("EMAIL, PASSWORD, ROLE, OPERATOR").eq("EMAIL", email).execute()

        if not response.data:
            return jsonify({"error": "User not found"}), 404

        user_data = response.data[0]
        stored_password = user_data["PASSWORD"]
        user_role = user_data["ROLE"]
        user_operator = user_data["OPERATOR"]

        # First check if password is correct
        if password != stored_password:
            return jsonify({"error": "Invalid password"}), 401

        # Next validate role selection
        if selected_role and selected_role != user_role:
            return jsonify({"error": "Invalid role selected for this account"}), 400

        # For telco users, validate operator selection
        if user_role == "telco" and selected_role == "telco":
            if selected_operator and selected_operator != user_operator:
                return jsonify({"error": "Invalid operator selected for this account"}), 400

        # Return successful login with user info
        return jsonify({
            "message": "Login successful", 
            "role": user_role,
            "operator": user_operator
        })

    except Exception as e:
        return jsonify({"error": f"Login failed: {str(e)}"}), 500

# ----------------------------------------
#  NEW ROUTE: Fetch MB_NETWORK Data (Synapse)
# ----------------------------------------
@app.route("/api/mb_network", methods=["GET"])
def fetch_mb_network():
    """
    Fetch data from MB_NETWORK (in Azure Synapse) with pagination.
    Endpoint: GET /api/mb_network?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch MB_NETWORK data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=0, type=int)   # default limit = 10
        offset = request.args.get("offset", default=0, type=int)    # default offset = 0

        print(f"📌 Fetching MB_NETWORK data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse
        df = get_mb_network_data(offset=offset, limit=limit)

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ MB_NETWORK data fetch successful!")
        df = clean_dataframe(df)
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_mb_network: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

# Add this route to app.py
@app.route("/api/mb_network/count", methods=["GET"])
def get_mb_network_count():
    """Get total count of MB_NETWORK records."""
    try:
        # This would be a new function in your connector
        from index_AzureSynapse_connector import get_mb_network_count
        
        count = get_mb_network_count()
        return jsonify({"count": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# -------------------------------------
#  NEW ROUTE: Fetch DATA_SOURCES Data 
# -------------------------------------
@app.route("/api/data_sources_filtered", methods=["GET"])
def get_data_sources_filtered():
    try:
        # Helper to clean floats
        def safe_float(val):
            try:
                return float(str(val).replace('>', '').replace('<', '').strip())
            except (ValueError, TypeError):
                return None

        def clean_df(df, source_name):
            """Add source column and clean coordinates."""
            if df is None or df.empty:
                return pd.DataFrame()
            df['SOURCE'] = source_name
            if 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
                df['LATITUDE'] = df['LATITUDE'].apply(safe_float)
                df['LONGITUDE'] = df['LONGITUDE'].apply(safe_float)
                df = df.dropna(subset=['LATITUDE', 'LONGITUDE'])
            return df

        # --- Query parameters ---
        source = request.args.get("source", "All")
        state = request.args.get("state")
        district = request.args.get("district")
        limit = request.args.get("limit", default=1000, type=int)
        offset = request.args.get("offset", default=0, type=int)

        # --- Collect dataframes ---
        data_frames = []
        if source in ("All", "Mobile Network"):
            data_frames.append(clean_df(get_mb_network_data(offset=offset, limit=limit), 'Mobile Network'))
        if source in ("All", "RCI"):
            data_frames.append(clean_df(get_tower_structures_data(offset=offset, limit=limit), 'RCI'))
        if source in ("All", "Fiber Network"):
            data_frames.append(clean_df(get_fiber_optic_site_data(offset=offset, limit=limit), 'Fiber Network'))
        if source in ("All", "NADI"):
            data_frames.append(clean_df(get_pedi_data(offset=offset, limit=limit), 'NADI'))
        if source in ("All", "PUDO"):
            data_frames.append(clean_df(get_pudo_data(offset=offset, limit=limit), 'PUDO'))

        # --- Combine into one dataframe ---
        combined_df = pd.concat([df for df in data_frames if not df.empty], ignore_index=True) if data_frames else pd.DataFrame(columns=['LATITUDE', 'LONGITUDE', 'STATE', 'DISTRICT', 'SOURCE'])

        # --- Apply filters ---
        if state and state.lower() != "all":
            combined_df = combined_df[combined_df['STATE'].str.strip().str.lower() == state.strip().lower()]
        if district and district.lower() != "all":
            combined_df = combined_df[combined_df['DISTRICT'].str.strip().str.lower() == district.strip().lower()]

        # --- Final cleaning for JSON ---
        combined_df = clean_dataframe(combined_df)

        return jsonify({
            "data": combined_df.to_dict(orient="records"),
            "count": int(len(combined_df))
        })

    except Exception as e:
        print(f"❌ Error in get_data_sources_filtered: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------------------
#  NEW ROUTE: Fetch TOWER_STRUCTURES Data (Synapse)
# ----------------------------------------
@app.route("/api/tower_structures", methods=["GET"])
def fetch_tower_structures():
    """
    Fetch data from TOWER_STRUCTURES (in Azure Synapse) with pagination.
    Endpoint: GET /api/tower_structures?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch TOWER_STRUCTURES data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching TOWER_STRUCTURES data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse using the new function
        df = get_tower_structures_data(offset=offset, limit=limit)

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ TOWER_STRUCTURES data fetch successful!")
        df = clean_dataframe(df)
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_tower_structures: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ----------------------------------------
#  NEW ROUTE: Fetch Fiber Optic Site Data (Synapse)
# ----------------------------------------
@app.route("/api/fiber_optic_sites", methods=["GET"])
def fetch_fiber_optic_sites():
    """
    Fetch data from FIBER_OPTIC_SITE (in Azure Synapse) with pagination.
    Only includes the following parameters:
      REFID, SERVICE_PROVIDER, CATEGORY, PROJECT,
      STRUCTURE_TYPE (from STRUCTURE_TYPE_CODE), DISTRICT, MUKIM, DUN,
      PARLIAMENT, LONGITUDE, LATITUDE.
    Endpoint: GET /api/fiber_optic_sites?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch FIBER_OPTIC_SITE data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching FIBER_OPTIC_SITE data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse using the new function
        df = get_fiber_optic_site_data(offset=offset, limit=limit)

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ FIBER_OPTIC_SITE data fetch successful!")
        df = clean_dataframe(df)
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_fiber_optic_sites: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
#  NEW ROUTE: Fetch PUDO Data (Synapse)
# ----------------------------------------
@app.route("/api/pudo", methods=["GET"])
def fetch_pudo():
    """
    Fetch data from PUDO (in Azure Synapse) with pagination.
    Endpoint: GET /api/pudo?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch PUDO data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching PUDO data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse
        df = get_pudo_data(offset=offset, limit=limit)

        # If there's an error column in the DataFrame, handle it
        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        df = clean_dataframe(df)
        
        # Convert to list of dicts
        data_records = df.to_dict(orient="records")

        # Return JSON
        print("✅ PUDO data fetch successful!")
        return jsonify({
            "data": data_records,
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_pudo: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

# ----------------------------------------
#  NEW ROUTE: Fetch PEDI Data (Synapse)
# ----------------------------------------
@app.route("/api/pedi", methods=["GET"])
def fetch_pedi():
    """
    Fetch data from PEDI (in Azure Synapse) with pagination.
    Endpoint: GET /api/pedi?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch PEDI data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching PEDI data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse
        df = get_pedi_data(offset=offset, limit=limit)

        # If there's an error column in the DataFrame, handle it
        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        df = clean_dataframe(df)
        # Convert DataFrame to list of dicts
        data_records = df.to_dict(orient="records")

        # Return JSON
        print("✅ PEDI data fetch successful!")
        return jsonify({
            "data": data_records,
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_pedi: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
# NEW ROUTE: Fetch MB_MORAN_MOCN Data (Synapse)
# ----------------------------------------
@app.route("/api/mb_moran_mocn", methods=["GET"])
def fetch_mb_moran_mocn():
    """
    Fetch data from MB_MORAN_MOCN_FULL (in Azure Synapse) with pagination.
    Endpoint: GET /api/mb_moran_mocn?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch MB_MORAN_MOCN data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching MB_MORAN_MOCN data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse using the updated connector function
        df = get_mb_moran_mocn_data(offset=offset, limit=limit)

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ MB_MORAN_MOCN data fetch successful!")
        df = clean_dataframe(df)
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_mb_moran_mocn: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
#  Fetch Operator Structure Data (RCI Module - Azure)
# ----------------------------------------------------
@app.route("/api/operator_structure", methods=["GET"])
def fetch_operator_structure():
    """
    Fetch total structure count by OWNER (operator) from Azure Synapse.
    Supports filters: operator, state, district, mukim, dun (all optional).
    """
    import os
    import pyodbc
    import pandas as pd

    # --- Database connection config ---
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    port = os.getenv('DB_PORT')
    driver = os.getenv('DB_DRIVER')

    try:
        # --- Fetch filter params from request ---
        operator = request.args.get("operator", default=None)
        state = request.args.get("state", default=None)
        district = request.args.get("district", default=None)
        mukim = request.args.get("mukim", default=None)
        dun = request.args.get("dun", default=None)

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        connection = pyodbc.connect(conn_str)
        cursor = connection.cursor()

        # --- Build dynamic WHERE clause for filters ---
        filters = ["STATUS != 'DISCONTINUE'"]
        if operator: filters.append(f"OPERATOR = '{operator}'")
        if state: filters.append(f"STATE = '{state}'")
        if district: filters.append(f"DISTRICT = '{district}'")
        if mukim: filters.append(f"MUKIM = '{mukim}'")
        if dun: filters.append(f"DUN = '{dun}'")
        where_clause = " AND ".join(filters)

        # --- Query with calculated percentage ---
        query = f"""
        WITH cte AS (
            SELECT 
                OWNER,
                COUNT(DISTINCT STRUCTURE_ID) AS [Total RCI]
            FROM [Dedicated SQL Pool].cims_geo.TOWER_STRUCTURES
            WHERE {where_clause}
            GROUP BY OWNER
        ),
        total_sum AS (
            SELECT SUM([Total RCI]) AS grand_total FROM cte
        )
        SELECT 
            cte.OWNER,
            cte.[Total RCI],
            FORMAT(ROUND((cte.[Total RCI] * 100.0 / NULLIF(total_sum.grand_total,0)), 2), 'N2') + '%' AS [Total RCI (%)]
        FROM cte
        CROSS JOIN total_sum
        ORDER BY cte.[Total RCI] DESC;
        """

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame.from_records(data, columns=columns)

        cursor.close()
        connection.close()
        return jsonify({"data": df.to_dict(orient="records")})

    except Exception as e:
        print(f"❌ Error in fetch_operator_structure: {e}")
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------------
#  New API Endpoint for Structure Category Data (RCI Module - Azure)
# ------------------------------------------------------------------
from flask import request

@app.route("/api/structure_category", methods=["GET"])
def fetch_structure_category():
    """
    Fetch total structure count by category from Azure Synapse.
    Supports filtering via query parameters.
    """
    try:
        print("\n🔍 Debug: Fetching structure count by STRUCTURE_CATEGORY...")

        # Get filters from query string
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")

        df = get_structure_category_data(
            operator=operator,
            state=state,
            district=district,
            mukim=mukim,
            dun=dun
        )

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ Structure by STRUCTURE_CATEGORY fetch successful.")
        return jsonify({"data": df.to_dict(orient="records")})

    except Exception as e:
        print(f"❌ Exception in fetch_structure_category: {str(e)}")
        return jsonify({"error": str(e)}), 500
# ------------------------------------------------------------------
#  New API Endpoint for Total Owner by Projects Data (RCI Module - Azure)
# ------------------------------------------------------------------

@app.route("/api/structure_project", methods=["GET"])
def fetch_structure_project():
    """
    Fetch total structure count by PROJECTS from Azure Synapse.
    Applies filter parameters if provided.
    Endpoint: GET /api/structure_project
    """
    try:
        # Grab filter params from query string
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")

        df = get_structure_project_data(
            operator=operator,
            state=state,
            district=district,
            mukim=mukim,
            dun=dun
        )

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            return jsonify({"error": error_message}), 500

        return jsonify({"data": df.to_dict(orient="records")})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
#  New API Endpoint for Total RCI by State Data (RCI Module - Azure)
# ------------------------------------------------------------------

@app.route("/api/structure_state", methods=["GET"])
def fetch_structure_state():
    """
    Fetch total structure count by STATE from Azure Synapse.
    Supports filtering via query parameters.
    """
    try:
        print("\n🔍 Debug: Fetching structure count by STATE...")

        # Get filters from query string
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")

        # Pass filters to the query function
        df = get_structure_state_data(
            operator=operator,
            state=state,
            district=district,
            mukim=mukim,
            dun=dun
        )

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ Structure by STATE fetch successful.")
        return jsonify({"data": df.to_dict(orient="records")})

    except Exception as e:
        print(f"❌ Exception in fetch_structure_state: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------------
#  New API Endpoint for Headers Data (RCI Module - Azure)
# ------------------------------------------------------------------

@app.route("/api/structure_summary", methods=["GET"])
def fetch_structure_summary():
    """
    Fetch summary metrics from TOWER_STRUCTURES.
    Endpoint: GET /api/structure_summary
    """
    try:
         # Get filters from query params
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")

        print("\n🔍 Debug: Fetching structure summary KPIs...")

        df = get_structure_summary_data(
            operator=operator,
            state=state,
            district=district,
            mukim=mukim,
            dun=dun
        )

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            return jsonify({"error": error_message}), 500

        print("✅ Structure summary fetch successful.")
        return jsonify({"data": df.to_dict(orient="records")[0]})  # return single row as object

    except Exception as e:
        print(f"❌ Exception in fetch_structure_summary: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

# -------------------------------------------------------------------
#  NEW ROUTE: Fetch TOWER_STRUCTURES Data for Map (RCI Module - Azure)
# -------------------------------------------------------------------
@app.route("/api/tower_structures_map", methods=["GET"])
def fetch_tower_structures_map():
    """
    Fetch data from TOWER_STRUCTURES (in Azure Synapse) for map display with pagination.
    Endpoint: GET /api/tower_structures_map?limit=10&offset=0
    """
    try:
        print("\n🔍 Debug: Received request to fetch TOWER_STRUCTURES map data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=1000, type=int)  # Higher default for map
        offset = request.args.get("offset", default=0, type=int)

        print(f"📌 Fetching TOWER_STRUCTURES map data with limit={limit}, offset={offset}")

        # Retrieve data from Synapse using the map function
        df = get_tower_structures_data_map(offset=offset, limit=limit)

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ TOWER_STRUCTURES map data fetch successful!")
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_tower_structures_map: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

# ----------------------------------------
#  NEW ROUTE: Get Filter Options for RCI Dashboard
# ----------------------------------------
@app.route("/api/tower_structures/filter_options", methods=["GET"])
def get_tower_structures_filter_options():
    """
    Get distinct values for filter dropdowns (operator, state, district, mukim, dun).
    Endpoint: GET /api/tower_structures/filter_options
    """
    try:
        print("\n🔍 Debug: Received request to fetch filter options for TOWER_STRUCTURES.")
        
        # Import the function that will query the database
        from RCI_AzureSynapse_connector import get_tower_structures_filter_options
        
        # Retrieve filter options from Synapse
        filter_options = get_tower_structures_filter_options()
        
        if isinstance(filter_options, dict) and "error" in filter_options:
            error_message = filter_options["error"]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500
        
        print("✅ Filter options fetch successful!")
        return jsonify(filter_options)
        
    except Exception as e:
        print(f"❌ Exception in get_tower_structures_filter_options: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
#  NEW ROUTE: Get Filtered Tower Structures Data
# ----------------------------------------
@app.route("/api/tower_structures/filtered", methods=["GET"])
def get_filtered_tower_structures():
    """
    Fetch filtered tower structures data based on selected filters.
    **ENHANCED with better empty filter handling**
    """
    try:
        print("\n🔍 Debug: Received request to fetch filtered TOWER_STRUCTURES data.")
        
        # Get filter parameters
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")
        
        # Get pagination parameters
        limit = request.args.get("limit", default=1000, type=int)
        offset = request.args.get("offset", default=0, type=int)
        
        print(f"📌 Raw parameters received:")
        print(f"   operator: '{operator}' (type: {type(operator)})")
        print(f"   state: '{state}' (type: {type(state)})")
        print(f"   district: '{district}' (type: {type(district)})")
        print(f"   mukim: '{mukim}' (type: {type(mukim)})")
        print(f"   dun: '{dun}' (type: {type(dun)})")
        
        # **CRITICAL FIX: Don't pass empty string parameters**
        # Only pass parameters that have meaningful values
        filter_params = {}
        
        def is_meaningful_param(value):
            """Check if parameter has meaningful value"""
            if value is None:
                return False
            str_value = str(value).strip().lower()
            if str_value in ['', 'none', 'null', 'undefined']:
                return False
            if str_value.startswith('all '):
                return False
            return True
        
        if is_meaningful_param(operator):
            filter_params['operator'] = operator.strip()
        if is_meaningful_param(state):
            filter_params['state'] = state.strip()
        if is_meaningful_param(district):
            filter_params['district'] = district.strip()
        if is_meaningful_param(mukim):
            filter_params['mukim'] = mukim.strip()
        if is_meaningful_param(dun):
            filter_params['dun'] = dun.strip()
        
        print(f"📋 Cleaned parameters: {filter_params}")
        print(f"🏷️ Has meaningful filters: {len(filter_params) > 0}")
        
        # Call the connector function with cleaned parameters
        df = get_tower_structures_filtered(
            operator=filter_params.get('operator'),
            state=filter_params.get('state'),
            district=filter_params.get('district'),
            mukim=filter_params.get('mukim'),
            dun=filter_params.get('dun'),
            offset=offset,
            limit=limit
        )
        
        # Check for errors in the DataFrame
        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500
        
        result_count = len(df)
        print(f"✅ Data fetch successful! Returned {result_count} records")
        
        # Prepare response with detailed information
        response_data = {
            "data": df.to_dict(orient="records"),
            "count": result_count,
            "has_filters": len(filter_params) > 0,
            "filters_applied": filter_params,
            "all_filters_received": {
                "operator": operator,
                "state": state,
                "district": district,
                "mukim": mukim,
                "dun": dun
            },
            "limit": limit,
            "offset": offset,
            "status": "success"
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Exception in get_filtered_tower_structures: {str(e)}")
        return jsonify({
            "error": str(e),
            "status": "error",
            "count": 0,
            "data": []
        }), 500
    
# ----------------------------------------
#  NEW ROUTE: Get Dependent Filter Options
# ----------------------------------------
@app.route("/api/tower_structures/dependent_filters", methods=["GET"])
def get_dependent_filter_options():
    """
    Get dependent filter options based on parent selections.
    Endpoint: GET /api/tower_structures/dependent_filters?state=
    """
    try:
        print("\n🔍 Debug: Received request to fetch dependent filter options.")
        
        # Get parent filter values
        state = request.args.get("state")
        
        # Import the function that will query the database
        from RCI_AzureSynapse_connector import get_dependent_filter_options
        
        # Retrieve dependent filter options from Synapse
        filter_options = get_dependent_filter_options(state=state)
        
        if isinstance(filter_options, dict) and "error" in filter_options:
            error_message = filter_options["error"]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500
        
        print("✅ Dependent filter options fetch successful!")
        return jsonify(filter_options)
        
    except Exception as e:
        print(f"❌ Exception in get_dependent_filter_options: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
# ============================================================================
# ADD NEW DEBUG ENDPOINT - Add this new route to app.py
# ============================================================================

@app.route("/api/debug/filters", methods=["GET"])
def debug_filters():
    """Debug endpoint to test filter handling"""
    try:
        # Get filter parameters exactly as the main endpoint does
        operator = request.args.get("operator")
        state = request.args.get("state")
        district = request.args.get("district")
        mukim = request.args.get("mukim")
        dun = request.args.get("dun")
        
        # Show exactly what was received
        debug_info = {
            "raw_params": {
                "operator": repr(operator),  # repr shows quotes and None
                "state": repr(state),
                "district": repr(district),
                "mukim": repr(mukim),
                "dun": repr(dun)
            },
            "param_types": {
                "operator": str(type(operator)),
                "state": str(type(state)),
                "district": str(type(district)),
                "mukim": str(type(mukim)),
                "dun": str(type(dun))
            },
            "cleaned_params": {},
            "validation_results": {}
        }
        
        # Apply the same cleaning logic as the main endpoint
        def clean_filter_value(value):
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned == "" or cleaned.lower() == "none" or cleaned.lower() == "null":
                    return None
                return cleaned
            return str(value).strip() if str(value).strip() else None
        
        # Clean each parameter
        params = {"operator": operator, "state": state, "district": district, "mukim": mukim, "dun": dun}
        for key, value in params.items():
            cleaned = clean_filter_value(value)
            debug_info["cleaned_params"][key] = cleaned
            debug_info["validation_results"][key] = {
                "original": repr(value),
                "cleaned": repr(cleaned),
                "is_active": cleaned is not None
            }
        
        # Check if any filters are active
        active_filters = {k: v for k, v in debug_info["cleaned_params"].items() if v is not None}
        debug_info["has_active_filters"] = len(active_filters) > 0
        debug_info["active_filter_count"] = len(active_filters)
        debug_info["active_filters"] = active_filters
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# ============================================================================
# ADD NEW TEST ENDPOINT - Add this new route to app.py
# ============================================================================

@app.route("/api/test/all_endpoints", methods=["GET"])
def test_all_endpoints():
    """Test all RCI endpoints to verify they're working"""
    try:
        test_results = {}
        
        # Test endpoints
        endpoints_to_test = [
            {"name": "structure_summary", "function": "get_structure_summary_data"},
            {"name": "structure_state", "function": "get_structure_state_data"},
            {"name": "structure_category", "function": "get_structure_category_data"},
            {"name": "structure_project", "function": "get_structure_project_data"},
            {"name": "operator_structure", "function": "get_operator_structure_data", "args": [10, 0]},
        ]
        
        for endpoint in endpoints_to_test:
            try:
                print(f"🧪 Testing {endpoint['name']}...")
                
                # Import the function dynamically
                from RCI_AzureSynapse_connector import (
                    get_structure_summary_data,
                    get_structure_state_data,
                    get_structure_category_data,
                    get_structure_project_data,
                    get_operator_structure_data
                )
                
                func = locals()[endpoint["function"]]
                
                # Call the function
                if "args" in endpoint:
                    result = func(*endpoint["args"])
                else:
                    result = func()
                
                # Check if result has errors
                if hasattr(result, 'columns') and "error" in result.columns:
                    test_results[endpoint["name"]] = {
                        "status": "error",
                        "error": result["error"].iloc[0] if len(result) > 0 else "Unknown error"
                    }
                else:
                    record_count = len(result) if hasattr(result, '__len__') else 1
                    test_results[endpoint["name"]] = {
                        "status": "success",
                        "record_count": record_count
                    }
                    
            except Exception as e:
                test_results[endpoint["name"]] = {
                    "status": "exception",
                    "error": str(e)
                }
        
        return jsonify({
            "test_results": test_results,
            "overall_status": "completed",
            "timestamp": pd.Timestamp.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# ============================================================================
# QUICK TEST: Add this route to test your API
# ============================================================================

@app.route("/api/test/all_data", methods=["GET"])
def test_all_data():
    """Test endpoint to verify unfiltered data"""
    try:
        # Get total count from structure summary
        summary_data = get_structure_summary_data()
        
        # Get sample unfiltered data
        sample_data = get_tower_structures_filtered(
            operator=None,
            state=None, 
            district=None,
            mukim=None,
            dun=None,
            offset=0,
            limit=10
        )
        
        return jsonify({
            "message": "Test successful",
            "total_structures": summary_data.get('TOTAL_STRUCTURES', 0) if hasattr(summary_data, 'get') else 0,
            "sample_count": len(sample_data),
            "sample_data": sample_data.to_dict(orient="records") if not sample_data.empty else []
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ----------------------------------------
#  Run the Flask server
# ----------------------------------------
if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)