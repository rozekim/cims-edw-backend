from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import supabase
from dotenv import load_dotenv
from passlib.context import CryptContext

# Load environment variables
load_dotenv()

# Validate environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are missing. Check your .env file.")

# Initialize Supabase client
supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Pydantic model for login request
class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/login")
async def login(user: LoginRequest):
    # Query Supabase for user credentials
    response = supabase_client.table("Login_EDW").select("EMAIL, PASSWORD, ROLE").eq("EMAIL", user.email).execute()

    if not response.data:
        raise HTTPException(status_code=404, detail="User not found")

    stored_password = response.data[0]["PASSWORD"]
    role = response.data[0]["ROLE"]

    # Verify password
    if not pwd_context.verify(user.password, stored_password):
        raise HTTPException(status_code=401, detail="Invalid password")

    return {"message": "Login successful", "role": role}
