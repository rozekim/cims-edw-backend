from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import supabase
from dotenv import load_dotenv
from RCI_Oracle_connector import fetch_operator_structure  # Import the function

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

# Home route
@app.route("/")
def home():
    return jsonify({"message": "Welcome to MCMC API!"})

# Fetch Operator Structure Data with Pagination
@app.route("/api/operator_structure", methods=["GET"])
def operator_structure():
    print("✅ Received request for /api/operator_structure")  # Debugging print
    return fetch_operator_structure()

# Run the Flask server
if __name__ == "__main__":
    app.run(debug=True, port=5000)
