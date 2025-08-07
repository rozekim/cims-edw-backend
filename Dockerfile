# ✅ Step 1: Base image
FROM python:3.9-slim

# ✅ Step 2: Install system dependencies & Microsoft ODBC Driver 18
RUN apt-get update && \
    apt-get install -y curl gnupg apt-transport-https unixodbc unixodbc-dev && \
    curl -sSL https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/microsoft.gpg > /dev/null
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm microsoft.gpg

# ✅ Step 3: Set working directory
WORKDIR /app

# ✅ Step 4: Copy your backend code
COPY . .

# ✅ Step 5: Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# ✅ Step 6: Expose Flask default port (optional)
EXPOSE 5000

# ✅ Step 7: Start your Flask app
CMD ["python", "app.py"]
