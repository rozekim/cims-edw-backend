# import cx_Oracle
# import os
# from dotenv import load_dotenv
# import pandas as pd
# from flask import request, jsonify

# # Load environment variables
# load_dotenv()

# # Get credentials from environment variables
# username = os.getenv('Username_Oracle')
# password = os.getenv('Password_Oracle')

# # Initialize Oracle Client
# cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")

# def get_operator_structure_data(limit, offset):
#     """Fetch distinct operator structure data with pagination."""
#     try:
#         print("\n🔍 Debug: Connecting to Database...")
#         dsn = cx_Oracle.makedsn(
#             host="192.168.11.198",
#             port=1521,
#             service_name="mcmccims"
#         )
        
#         connection = cx_Oracle.connect(
#             user=username,
#             password=password,
#             dsn=dsn
#         )
#         print("✅ Database Connection Successful!")

#         cursor = connection.cursor()
        
#         # Query to count distinct OPERATOR structures with pagination
#         query = f"""
#         SELECT OPERATOR AS OWNER,
#                COUNT(DISTINCT STRUCTURE_ID) AS "Total RCI",
#                ROUND((COUNT(DISTINCT STRUCTURE_ID) / SUM(COUNT(DISTINCT STRUCTURE_ID)) OVER()) * 100, 2) || '%' AS "Total RCI (%)"
#         FROM EDW.V_TOWERS_STRUCTURES
#         GROUP BY OPERATOR
#         ORDER BY "Total RCI" DESC
#         OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
#         """

#         cursor.execute(query)

#         # Get column names and data
#         columns = [col[0] for col in cursor.description]
#         data = cursor.fetchall()

#         # Convert to pandas DataFrame
#         df = pd.DataFrame(data, columns=columns)

#         cursor.close()
#         connection.close()
#         return df

#     except cx_Oracle.Error as error:
#         print(f"\n❌ Database Error: {error}")
#         return pd.DataFrame({"error": [str(error)]})

# def fetch_operator_structure():
#     """Fetch operator structure data from the Oracle database with pagination."""
#     try:
#         print("\n🔍 Debug: Received request to fetch operator structure data.")

#         # Get pagination parameters
#         limit = request.args.get("limit", default=10, type=int)  # Default limit: 10 rows
#         offset = request.args.get("offset", default=0, type=int)  # Default offset: 0

#         print(f"📌 Fetching data with limit={limit}, offset={offset}")

#         # Fetch paginated data
#         data = get_operator_structure_data(limit, offset)

#         if isinstance(data, dict) and "error" in data:
#             print(f"❌ Error fetching data: {data['error']}")
#             return jsonify(data), 500  # Return error with status code 500

#         print("✅ Data fetch successful!")
#         return jsonify({"data": data.to_dict(orient="records"), "limit": limit, "offset": offset})

#     except Exception as e:
#         print(f"❌ Exception occurred: {str(e)}")
#         return jsonify({"error": f"Failed to fetch data: {str(e)}"}), 500

# # Example usage
# if __name__ == "__main__":
#     df_result = fetch_operator_structure()
#     print(df_result)  # Print DataFrame

import cx_Oracle
import os
from dotenv import load_dotenv
import pandas as pd
from flask import request, jsonify

# Load environment variables
load_dotenv()

# Get credentials from environment variables
username = os.getenv('Username_Oracle')
password = os.getenv('Password_Oracle')
oracle_host = "192.168.11.198"
oracle_port = 1521
oracle_service = "mcmccims"

# Initialize Oracle Client
try:
    cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")
    print("✅ Oracle Client Initialized Successfully!")
except Exception as e:
    print(f"❌ Failed to initialize Oracle client: {e}")

def get_operator_structure_data(limit, offset):
    """Fetch distinct operator structure data with pagination."""
    try:
        print("\n🔍 Debug: Connecting to Oracle Database...")
        dsn = cx_Oracle.makedsn(
            host=oracle_host,
            port=oracle_port,
            service_name=oracle_service
        )
        
        connection = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        print("✅ Database Connection Successful!")

        cursor = connection.cursor()

        # Use ROW_NUMBER() for pagination instead of OFFSET/FETCH for better Oracle compatibility
        # query = f"""
        # SELECT OWNER, "Total RCI", "Total RCI (%)"
        # FROM (
        #     SELECT OPERATOR AS OWNER,
        #            COUNT(DISTINCT STRUCTURE_ID) AS "Total RCI",
        #            ROUND((COUNT(DISTINCT STRUCTURE_ID) / SUM(COUNT(DISTINCT STRUCTURE_ID)) OVER()) * 100, 2) || '%' AS "Total RCI (%)",
        #            ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT STRUCTURE_ID) DESC) AS rn
        #     FROM EDW.V_TOWERS_STRUCTURES
        #     GROUP BY OPERATOR
        # ) WHERE rn BETWEEN {offset + 1} AND {offset + limit}
        # """
        query = f"""
        SELECT OPERATOR AS OWNER,
               COUNT(DISTINCT STRUCTURE_ID) AS "Total RCI",
               ROUND((COUNT(DISTINCT STRUCTURE_ID) / SUM(COUNT(DISTINCT STRUCTURE_ID)) OVER()) * 100, 2) || '%' AS "Total RCI (%)"
        FROM EDW.V_TOWERS_STRUCTURES
        GROUP BY OPERATOR
        ORDER BY "Total RCI" DESC
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        cursor.execute(query)

        # Get column names and data
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()

        # Convert to pandas DataFrame
        df = pd.DataFrame(data, columns=columns)

        cursor.close()
        connection.close()
        print("✅ Data retrieval successful!")
        return df

    except cx_Oracle.DatabaseError as error:
        print(f"\n❌ Database Error: {error}")
        return pd.DataFrame({"error": [str(error)]})

    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        return pd.DataFrame({"error": [str(e)]})

def fetch_operator_structure():
    """Fetch operator structure data from the Oracle database with pagination."""
    try:
        print("\n🔍 Debug: Received request to fetch operator structure data.")

        # Get pagination parameters
        limit = request.args.get("limit", default=10, type=int)  # Default limit: 10 rows
        offset = request.args.get("offset", default=0, type=int)  # Default offset: 0

        print(f"📌 Fetching data with limit={limit}, offset={offset}")

        # Fetch paginated data
        data = get_operator_structure_data(limit, offset)

        if isinstance(data, dict) and "error" in data:
            print(f"❌ Error fetching data: {data['error']}")
            return jsonify(data), 500  # Return error with status code 500

        print("✅ Data fetch successful!")
        return jsonify({"data": data.to_dict(orient="records"), "limit": limit, "offset": offset})

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return jsonify({"error": f"Failed to fetch data: {str(e)}"}), 500

# Example usage (for debugging purposes)
if __name__ == "__main__":
    df_result = get_operator_structure_data(100, 0)  # Fetch first 100 records
    print(df_result)  # Print DataFrame output
