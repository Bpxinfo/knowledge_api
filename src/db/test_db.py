import os
import aiomysql
import asyncio
import ssl
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# async def create_db_connection():
#     """Async method to create database connection pool and verify connection."""
#     try:
#         # Create a default SSL context
#         # ssl_context = ssl.create_default_context()
#         pool = await aiomysql.create_pool(
#             host=os.getenv('host'),
#             port=int(os.getenv('DB_PORT', 3306)),
#             user=os.getenv('user'),
#             password=os.getenv('password'),
#             db=os.getenv('database'),
#             # ssl=ssl_context,  # Enable SSL
#             autocommit=True
#         )
        
#         # Test the connection by executing a simple query
#         async with pool.acquire() as conn:
#             async with conn.cursor() as cursor:
#                 await cursor.execute("SELECT 1")
#                 result = await cursor.fetchone()
#                 if result:
#                     print("Database connection successful.")
        
#         return pool
    
#     except Exception as e:
#         print(f"Database connection error: {e}")
#         raise


# async def main():
#     pool = await create_db_connection()
#     if pool:
#         print("Connection pool created successfully.")
#     # Close the pool after use
#     pool.close()
#     await pool.wait_closed()

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except Exception as e:
#         print(f"Error running the async main function: {e}")

import os
import re
import json
import mysql.connector
from datetime import datetime
from dateutil.parser import parse

def create_db_connection():
    """Method to create database connection"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('host'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('user'),
            password=os.getenv('password'),
            database=os.getenv('database')
        )
        return connection
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
        raise



import os
import re
import json
import mysql.connector
from datetime import datetime
from dateutil.parser import parse


def create_table(connection, table_name, columns, foreign_key=None):
    """
    Create a table with the specified columns and optional foreign key.
    
    Args:
        connection: Database connection
        table_name: Name of the table to create
        columns: List of column definitions
        foreign_key: Optional foreign key constraint
    """
    cursor = connection.cursor()
    
    # Sanitize and prepare columns
    sanitized_columns = [col.replace('VARCHAR(255)', 'VARCHAR(512)') for col in columns]
    
    # Create table query with explicit ID column
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(sanitized_columns)}
        {', ' + foreign_key if foreign_key else ''}
    ) ENGINE=InnoDB;
    """
    
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' created successfully!")
    except mysql.connector.Error as e:
        print(f"Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()

def get_sql_type(value):
    """Map Python data types to MySQL data types."""
    if value is None:
        return "TEXT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        # Determine if the string is a DATE
        try:
            parsed_date = parse(value)
            return "DATETIME"
        except:
            pass
        return "TEXT" if len(value) > 512 else "VARCHAR(512)"
    elif isinstance(value, (list, dict)):
        return "JSON"
    else:
        return "TEXT"

def sanitize_sheetname(sheetname):
    """Sanitize the sheet name to make it a valid MySQL table name."""
    sanitized = re.sub(r'\W+', '_', str(sheetname))  # Replace non-word characters with underscores
    return sanitized.lower().strip('_')

def create_tables_dynamically(data, sheetname):
    """
    Create all necessary tables based on the JSON data structure.
    
    Args:
        data (list or str): JSON data or path to JSON file
        sheetname (str): Name of the sheet/table to be created
    """
    # Load data if a file path is provided
    if isinstance(data, str):
        try:
            with open(data, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return None
    else:
        json_data = data

    # Validate input
    if not json_data:
        print("No data provided to analyze.")
        return None

    try:
        # Create database connection
        connection = create_db_connection()
        
        # Sanitize sheet name to prevent SQL injection
        sanitized_sheetname = sanitize_sheetname(sheetname)
        
        # Define table names
        table_name = f"`{sanitized_sheetname}`"
        nodes_table = f"`{sanitized_sheetname}_nodes`"
        edges_table = f"`{sanitized_sheetname}_edges`"

        # Process the first record to get structure
        first_record = json_data[0]

        # 1. Create feedback table
        feedback_columns = []
        for key, value in first_record.items():
            # Exclude nested structures that will be in separate tables
            if key not in ['nodes', 'edges', 'Tags']:  
                sql_type = get_sql_type(value)
                feedback_columns.append(f"`{key}` {sql_type}")
        
        create_table(connection, table_name, feedback_columns)

        # 2. Create nodes table
        nodes_columns = [
            "node_id VARCHAR(255)",
            "label VARCHAR(255)",
            "type VARCHAR(255)",
            f"feedback_id INT"
        ]
        nodes_foreign_key = f"CONSTRAINT fk_feedback FOREIGN KEY (feedback_id) REFERENCES {table_name}(id) ON DELETE CASCADE"
        create_table(connection, nodes_table, nodes_columns, nodes_foreign_key)

        # 3. Create edges table
        edges_columns = [
            "source VARCHAR(255)",
            "target VARCHAR(255)", 
            "relationship VARCHAR(255)",
            f"feedback_id INT"
        ]
        edges_foreign_key = f"CONSTRAINT fk_feedback_edges FOREIGN KEY (feedback_id) REFERENCES {table_name}(id) ON DELETE CASCADE"
        create_table(connection, edges_table, edges_columns, edges_foreign_key)

        print(f"All tables created successfully for '{sanitized_sheetname}'!")
        
        # Close the connection
        connection.close()
        
        return json_data

    except Exception as e:
        print(f"Error creating tables: {e}")
        raise

def main():
    # Configuration
    data_path = r"C:\Users\nickc\OneDrive\Desktop\New folder\backend\artifact\_5sec.json"
    sheet_name = "2024 Feedback and Quotes"

    # Run extraction and table creation
    processed_records = create_tables_dynamically(data_path, sheetname=sheet_name)

if __name__ == "__main__":
    main()