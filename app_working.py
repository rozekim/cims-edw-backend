from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import pandas as pd
import supabase
from dotenv import load_dotenv

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
    get_structure_category_data
)

# Load environment variables
load_dotenv()

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

# -------------------------------------------------
#  Fetch Operator Structure Data RCI Module (Azure)
# -------------------------------------------------
@app.route("/api/operator_structure", methods=["GET"])
def fetch_operator_structure():
    """Fetch operator structure data from the Azure database with pagination."""
    try:
        print("\n🔍 Debug: Received request to fetch operator structure data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=20, type=int)  # Default limit: 100 rows
        offset = request.args.get("offset", default=0, type=int)  # Default offset: 0

        print(f"📌 Fetching data with limit={limit}, offset={offset}")

        # Fetch paginated data (Azure connector)
        data = get_operator_structure_data(limit, offset)

        # Check for errors
        if hasattr(data, 'error'):
            print(f"❌ Error fetching data: {data.error}")
            return jsonify({"error": data.error}), 500

        print("✅ Data fetch successful!")
        return jsonify({
            "data": data.to_dict(orient="records") if isinstance(data, pd.DataFrame) else data,
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({"error": f"Failed to fetch data: {str(e)}"}), 500

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
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_mb_network: {str(e)}")
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
        return jsonify({
            "data": df.to_dict(orient="records"),
            "limit": limit,
            "offset": offset
        })

    except Exception as e:
        print(f"❌ Exception in fetch_mb_moran_mocn: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------------
#  New API Endpoint for Structure Category Data (RCI Module)
# ----------------------------------------------------------
@app.route("/api/structure_category", methods=["GET"])
def fetch_structure_category():
    """
    Fetch total structure count by structure category from Azure Synapse.
    Endpoint: GET /api/structure_category
    """
    try:
        print("\n🔍 Debug: Received request to fetch Structure Category data.")

        # Retrieve data from Synapse
        df = get_structure_category_data()

        if "error" in df.columns:
            error_message = df["error"].iloc[0]
            print(f"❌ Error returned from query: {error_message}")
            return jsonify({"error": error_message}), 500

        print("✅ Structure Category data fetch successful!")
        return jsonify({"data": df.to_dict(orient="records")})

    except Exception as e:
        print(f"❌ Exception in fetch_structure_category: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
#  Run the Flask server
# ----------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)