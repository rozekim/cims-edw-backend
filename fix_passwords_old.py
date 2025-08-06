import supabase
import os
from passlib.context import CryptContext
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Initialize Supabase client
supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Fetch all users
response = supabase_client.table("Login_EDW").select("EMAIL, PASSWORD").execute()
users = response.data

for user in users:
    email = user["EMAIL"]
    plaintext_password = user["PASSWORD"]

    # Check if already hashed
    if not plaintext_password.startswith("$2b$"):
        hashed_password = pwd_context.hash(plaintext_password)

        # Update password in Supabase
        supabase_client.table("Login_EDW").update({
            "PASSWORD": hashed_password
        }).eq("EMAIL", email).execute()
        
        print(f"Updated password for {email}")

print("Password update complete.")
