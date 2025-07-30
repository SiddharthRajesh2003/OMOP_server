from sqlalchemy.engine import create_engine
import pyodbc
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

load_dotenv()

# Load environment variables from .env file
user = os.getenv('UID')
pwd = os.getenv('PWD')

# Function to create a database connection string with parameters using ODBC
def connect_params_for_sql_server(db_url: str, db_name: str):
    params = quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_url};"
        f"DATABASE={db_name};"
        f"UID={user};"
        f"PWD={pwd};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Authentication=ActiveDirectoryPassword;"
        f"autocommit=True;"  # Add autocommit to handle transaction issues
    )
    return params

# Function to create a database engine using SQLAlchemy
def connect_db(db_url: str, db_name: str):
    params = connect_params_for_sql_server(db_url, db_name)
    # Add isolation_level="AUTOCOMMIT" to handle SQL Server transaction issues
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        isolation_level="AUTOCOMMIT"
    )
    return engine