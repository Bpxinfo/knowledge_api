import mysql.connector
import json
import time

import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.utils import sanitize_sheetname, get_sql_type

load_dotenv()

HOST= os.getenv('host')
# port = os.getenv('port')
USER = os.getenv('user')
PASSWORD = os.getenv('password')
DATABASE = os.getenv('database')

# def create_connection():
#     return mysql.connector.connect(
#            host="127.0.0.1",
#             port=3306,
#             user="root",
#             password='1234',
#             database="graph_database" # Database name
#     )
def create_connection():
    return mysql.connector.connect(
            host= HOST,
            user = USER,
            password = PASSWORD,
            database = DATABASE,
        )



def create_table(cursor, table_name, columns):
    """Create a table with the specified columns."""

    # Check for the 'stopwords' column and make it TEXT if it's defined as VARCHAR
    if 'stopwords' in columns:
        columns = [col.replace('VARCHAR(255)', 'TEXT') if 'stopwords' in col else col for col in columns]
        
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(columns)}
    );
    """
    cursor.execute(create_table_query)
    print(f"Table '{table_name}' created successfully!")


def check_table_exists(sheetname):
    """
    Checks if a given table exists in the connected MySQL database.

    Args:
        db_connection (mysql.connector.connection.MySQLConnection): The connection object to the database.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        db = create_connection()
        sanitized_sheetname = sanitize_sheetname(sheetname)
        cursor = db.cursor()
        cursor.execute("SHOW TABLES LIKE %s", (sanitized_sheetname,))
        return cursor.fetchone() is not None
    except:
        return False
    finally:
        cursor.close()

def create_tables_dynamically(json_data, sheetname):
    """Create all necessary tables based on the JSON data structure."""
    try:
        db = create_connection()
        cursor = db.cursor()

        sanitized_sheetname = sanitize_sheetname(sheetname)
        Table_name = f"`{sanitized_sheetname}`"
        nodes = f"`{sanitized_sheetname}_nodes`"
        edges = f"`{sanitized_sheetname}_edges`"

        # Process the first record to get structure
        if not json_data:
            print("No data provided to analyze.")
            return
        # Ensure json_data is a list of dictionaries
        if isinstance(json_data, str):
            json_data = json.loads(json_data)  # Convert JSON string to a Python dictionary/list

        if isinstance(json_data, dict):
            # If json_data is a dictionary, convert it into a list of dictionaries (single record)
            json_data = [json_data]

        first_record = json_data[0]

        # 1. Create feedback table
        feedback_columns = [
            "`sheet_name_id` INT",  # Add sheet_name_id column
            "FOREIGN KEY (sheet_name_id) REFERENCES `Sheet_name`(id) ON DELETE SET NULL ON UPDATE CASCADE" , # Add foreign key constraint
        ]

        for key, value in first_record.items():
            if key not in ['nodes', 'edges']:  # Exclude these as they'll be separate tables
                sql_type = get_sql_type(value)
                feedback_columns.append(f"`{key}` {sql_type}")
        create_table(cursor, Table_name, feedback_columns)

        # 2. Create nodes table
        nodes_columns = [
            "`feedback_id` INT",
            "`node_id` VARCHAR(255)",
            "`label` VARCHAR(255)",
            "`type` VARCHAR(255)",
            f"FOREIGN KEY (feedback_id) REFERENCES {Table_name}(id)"
        ]
        create_table(cursor, nodes, nodes_columns)

        # 3. Create edges table
        edges_columns = [
            "`feedback_id` INT",
            "`source` VARCHAR(255)",
            "`target` VARCHAR(255)",
            "`relationship` VARCHAR(255)",
            f"FOREIGN KEY (feedback_id) REFERENCES {Table_name}(id)"
        ]
        create_table(cursor, edges, edges_columns)
        print(f"All tables created successfully for '{sanitized_sheetname}'!")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()









