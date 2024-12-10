
import mysql.connector
import json
import time
from dateutil.parser import parse
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.exception import CustomException
from src.logger import logging

load_dotenv()

HOST= os.getenv('host')
# port = os.getenv('port')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')


def create_connection():
    return mysql.connector.connect(
            host= HOST,
            user = USER,
            password = PASSWORD,
            database = DATABASE,
        )



def get_table_names():
    try:
        # Connect to MySQL
        connection = create_connection()
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES;")
            # print(cursor.fetchall())
            tables = [table[0] for table in cursor.fetchall()]
            return tables
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def fetch_data_from_table(table_name):
    try:
        # Connect to MySQL
        connection = create_connection()
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)  # Return data as a dictionary
            query = f"SELECT * FROM {table_name};"
            cursor.execute(query)
            records = cursor.fetchall()
            return records
    except Exception as e:
        return {"error": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()            



def fetch_user_table():
    try:
        connection = create_connection()
        # Connect to Azure MySQL
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)  # Fetch results as dictionaries
            query = "SELECT * FROM user;"
            cursor.execute(query)
            records = cursor.fetchall()
            return records
    except Exception as e:
        return {"error": str(e)}
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()            



def get_sheet_names_from_db():
    
    try:
        connection = create_connection()
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            # Check if the Sheet_name table exists
            query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = 'Sheet_name'
            """
            cursor.execute(query, (DATABASE,))
            table_exists = cursor.fetchone()['COUNT(*)'] > 0

            if not table_exists:
                return {
                    "message": "Sheet_name table does not exist. Please upload a data file.",
                    "status": "error",
                    "data": None
                }

            # Fetch data from the Sheet_name table
            cursor.execute("SELECT * FROM Sheet_name")
            data = cursor.fetchall()

            return {
                "message": "Data retrieved successfully.",
                "status": "success",
                "data": data
            }

    except Exception as e:
        return {
            "message": "An error occurred while retrieving data.",
            "status": "error",
            "error": str(e),
            "data": None
        }

    finally:
        if connection:
            connection.close()           


import re

def sanitize_sheetname(sheetname):
    """Sanitize the sheet name to make it a valid MySQL table name."""
    sanitized = re.sub(r'\W+', '_', sheetname)  # Replace non-word characters with underscores
    return sanitized.lower().strip('_') 

# Function to fetch data from the database
def fetch_table_data(sheet_name, id_value):
    try:
        # Connect to the database
        connection = create_connection()
        cursor = connection.cursor(dictionary=True)
        table_name = sanitize_sheetname(sheetname=sheet_name)
        # Query the table
        # query = f"SELECT * FROM {table_name} WHERE sheet_name_id = %s"
        query = f"SELECT * FROM {table_name}"

        # cursor.execute(query, (id_value,))
        cursor.execute(query,)

        result = cursor.fetchall()
        
        return result
    except Exception as e:
        raise Exception(f"Database query error: {str(e)}")
    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()            

# Function to fetch data from the database
def fetch_edges(sheet_name):
    try:
        table_name = sanitize_sheetname(sheetname=sheet_name)
        nodes = f"{table_name}_nodes"
        edges = f"{table_name}_edges"
        # Connect to the database
        connection = create_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Query the table
        query = f"SELECT * FROM {edges}"
        cursor.execute(query,)
        edges_result = cursor.fetchall()

        query = f"SELECT * FROM {nodes}"
        cursor.execute(query,)
        nodes_result = cursor.fetchall()
        
        return edges_result,nodes_result
    except Exception as e:
        raise Exception(f"Database query error: {str(e)}")
    finally:
        # Ensure resources are cleaned up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()               





