import mysql.connector
import json
import time
from dateutil.parser import parse
import os
from dotenv import load_dotenv
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

def get_sql_type(value):
    """Map Python data types to MySQL data types."""
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        # Determine if the string is a DATE
        try:
            parse(value)
            return "DATETIME"
        except:
            pass
        return "TEXT" if len(value) > 255 else "VARCHAR(255)"
    elif isinstance(value, (list, dict)):
        return "JSON"
    else:
        return "TEXT"

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

import re

def sanitize_sheetname(sheetname):
    """Sanitize the sheet name to make it a valid MySQL table name."""
    sanitized = re.sub(r'\W+', '_', sheetname)  # Replace non-word characters with underscores
    return sanitized.lower().strip('_')  # Convert to lowercase and trim leading/trailing underscores

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




def insert_data(json_data, sheetname):
    try:
        db = create_connection()
        cursor = db.cursor()

        sanitized_sheetname = sanitize_sheetname(sheetname)
        Table_name = f"`{sanitized_sheetname}`"
        nodes = f"`{sanitized_sheetname}_nodes`"
        edges = f"`{sanitized_sheetname}_edges`"
        tags = f"`{sanitized_sheetname}_tags`"

        for record in json_data:
            # 1. Insert into feedback table
            feedback_data = {k: v for k, v in record.items() if k not in ['nodes', 'edges', 'Tags']}
            columns = ', '.join(f"`{k}`" for k in feedback_data.keys())
            placeholders = ', '.join(['%s'] * len(feedback_data))
            insert_feedback_query = f"INSERT INTO {Table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_feedback_query, list(feedback_data.values()))
            feedback_id = cursor.lastrowid

            # 2. Insert nodes
            if 'nodes' in record:
                for node in record['nodes']:
                    insert_node_query = f"""
                    INSERT INTO {nodes} (feedback_id, node_id, label, type)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_node_query, (feedback_id, node['id'], node['label'], node['type']))

            # 3. Insert edges
            if 'edges' in record:
                for edge in record['edges']:
                    insert_edge_query = f"""
                    INSERT INTO {edges} (feedback_id, source, target, relationship)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_edge_query, (feedback_id, edge['source'], edge['target'], edge['relationship']))

            # 4. Insert tags
            # if 'Tags' in record:
            #     for tag in record['Tags']:
            #         insert_tag_query = f"""
            #         INSERT INTO {tags} (feedback_id, tag)
            #         VALUES (%s, %s)
            #         """
            #         cursor.execute(insert_tag_query, (feedback_id, tag))

        db.commit()
        print(f"Data inserted successfully for '{sanitized_sheetname}'!")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()


def row_old_insert_data(json_data, sheetname):
    try:
        db = create_connection()
        cursor = db.cursor()

        sanitized_sheetname = sanitize_sheetname(sheetname)
        Table_name = f"`{sanitized_sheetname}`"
        nodes = f"`{sanitized_sheetname}_nodes`"
        edges = f"`{sanitized_sheetname}_edges`"

        for record in json_data:
            # 1. Insert into main feedback table
            feedback_data = {
                k: v for k, v in record.items() 
                if k not in ['nodes', 'edges', 'Theme', 'Safety', 'Treatment', 
                             'Diagnosis', 'sentiment', 'AnalyzeThoroughly', 'Issue', 'keywords', 'stopwords', 'synonyms']
            }
            
            # Add additional fields to main table
            # feedback_data['Issue'] = record.get('Issue', None)

            # # feedback_data['Theme'] = record.get('Theme', None)[0]
            # #   # Assuming 'Theme' is a string or None
            # theme_value = record.get('Theme')
            # feedback_data['Theme'] = json.dumps(theme_value) if theme_value else 'null'
            # # Safely join lists or set None if empty/missing
            # # feedback_data['Theme'] = ', '.join(map(str, record.get('Theme', []))) if record.get('Theme') else None 
            # feedback_data['Keywords'] = ', '.join(map(str, record.get('keywords', []))) if record.get('keywords') else None
            # # feedback_data['Stopwords'] = ', '.join(map(str, record.get('stopwords', []))) if record.get('stopwords') else None
            # feedback_data['Stopwords'] = record.get('stopwords', None)
            # feedback_data['Synonyms'] = ', '.join(map(str, record.get('synonyms', []))) if record.get('synonyms') else None
            # feedback_data['Sentiment'] = ', '.join(map(str, record.get('sentiment', []))) if record.get('sentiment') else None
            # feedback_data['AnalyzeThoroughly'] = ', '.join(map(str, record.get('AnalyzeThoroughly', []))) if record.get('AnalyzeThoroughly') else None

            feedback_data = {
    k: v for k, v in record.items() 
    if k not in ['nodes', 'edges', 'Theme', 'Safety', 'Treatment', 
                 'Diagnosis', 'sentiment', 'AnalyzeThoroughly', 'Issue', 'keywords', 'stopwords', 'synonyms']
}

            # Convert lists to JSON strings
            feedback_data['Theme'] = json.dumps(record.get('Theme', [])) if record.get('Theme') else 'null'
            feedback_data['Safety'] = json.dumps(record.get('Safety', [])) if record.get('Safety') else 'null'
            feedback_data['Treatment'] = json.dumps(record.get('Treatment', [])) if record.get('Treatment') else 'null'
            feedback_data['Diagnosis'] = json.dumps(record.get('Diagnosis', [])) if record.get('Diagnosis') else 'null'
            feedback_data['sentiment'] = json.dumps(record.get('sentiment', [])) if record.get('sentiment') else 'null'
            feedback_data['AnalyzeThoroughly'] = json.dumps(record.get('AnalyzeThoroughly', [])) if record.get('AnalyzeThoroughly') else 'null'
            feedback_data['Issue'] = record.get('Issue', None)
            feedback_data['Keywords'] = json.dumps(record.get('keywords', [])) if record.get('keywords') else 'null'
            feedback_data['Stopwords'] = record.get('stopwords', None)
            feedback_data['Synonyms'] = json.dumps(record.get('synonyms', [])) if record.get('synonyms') else 'null'
            print(feedback_data)
            columns = ', '.join(f"`{k}`" for k in feedback_data.keys())
            placeholders = ', '.join(['%s'] * len(feedback_data))
            insert_feedback_query = f"INSERT INTO {Table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_feedback_query, list(feedback_data.values()))
            feedback_id = cursor.lastrowid

            # 2. Insert nodes
            if 'nodes' in record:
                for node in record['nodes']:
                    insert_node_query = f"""
                    INSERT INTO {nodes} (feedback_id, node_id, label, type)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_node_query, (feedback_id, node['id'], node['label'], node['type']))

            # 3. Insert edges
            if 'edges' in record:
                for edge in record['edges']:
                    insert_edge_query = f"""
                    INSERT INTO {edges} (feedback_id, source, target, relationship)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_edge_query, (feedback_id, edge['source'], edge['target'], edge['relationship']))

        db.commit()
        print(f"Data inserted successfully for '{sanitized_sheetname}'!")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()


import json
import mysql.connector

import json
import mysql.connector

def row_insert_data(json_data, sheetname):
    try:
        db = create_connection()
        cursor = db.cursor()

        sanitized_sheetname = sanitize_sheetname(sheetname)
        Table_name = f"`{sanitized_sheetname}`"
        nodes = f"`{sanitized_sheetname}_nodes`"
        edges = f"`{sanitized_sheetname}_edges`"

        # Ensure json_data is a list of dictionaries
        if isinstance(json_data, str):
            json_data = json.loads(json_data)  # Convert JSON string to a Python dictionary/list

        if isinstance(json_data, dict):
            # If json_data is a dictionary, convert it into a list of dictionaries (single record)
            json_data = [json_data]  # Wrap the single dictionary in a list

        for record in json_data:
            if not isinstance(record, dict):
                raise ValueError(f"Record is not a dictionary: {record}")

            # Process feedback data, excluding the nested keys ('nodes', 'edges', etc.)
            feedback_data = {
                k: v for k, v in record.items() 
                if k not in ['nodes', 'edges', 'Theme', 'Safety', 'Treatment', 
                             'Diagnosis', 'sentiment', 'AnalyzeThoroughly', 'Issue', 'keywords', 'stopwords', 'synonyms']
            }

            # Convert lists to JSON strings for fields
            feedback_data['Theme'] = json.dumps(record.get('Theme', [])) if record.get('Theme') else 'null'
            feedback_data['Tags'] = json.dumps(record.get('Tags', [])) if record.get('Tags') else 'null'
            feedback_data['Safety'] = json.dumps(record.get('Safety', [])) if record.get('Safety') else 'null'
            feedback_data['Treatment'] = json.dumps(record.get('Treatment', [])) if record.get('Treatment') else 'null'
            feedback_data['Diagnosis'] = json.dumps(record.get('Diagnosis', [])) if record.get('Diagnosis') else 'null'
            feedback_data['sentiment'] = json.dumps(record.get('sentiment', [])) if record.get('sentiment') else 'null'
            feedback_data['AnalyzeThoroughly'] = json.dumps(record.get('AnalyzeThoroughly', [])) if record.get('AnalyzeThoroughly') else 'null'
            feedback_data['Issue'] = record.get('Issue', None)
            feedback_data['Keywords'] = json.dumps(record.get('keywords', [])) if record.get('keywords') else 'null'
            feedback_data['Stopwords'] = record.get('stopwords', None)
            feedback_data['Synonyms'] = json.dumps(record.get('synonyms', [])) if record.get('synonyms') else 'null'
            print(feedback_data)

            # Construct and execute the insert query for feedback data
            columns = ', '.join(f"`{k}`" for k in feedback_data.keys())
            placeholders = ', '.join(['%s'] * len(feedback_data))
            insert_feedback_query = f"INSERT INTO {Table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_feedback_query, list(feedback_data.values()))
            feedback_id = cursor.lastrowid

            # 2. Insert nodes if available
            if 'nodes' in record:
                for node in record['nodes']:
                    insert_node_query = f"""
                    INSERT INTO {nodes} (feedback_id, node_id, label, type)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_node_query, (feedback_id, node['id'], node['label'], node['type']))

            # 3. Insert edges if available
            if 'edges' in record:
                for edge in record['edges']:
                    insert_edge_query = f"""
                    INSERT INTO {edges} (feedback_id, source, target, relationship)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_edge_query, (feedback_id, edge['source'], edge['target'], edge['relationship']))

        db.commit()
        print(f"Data inserted successfully for '{sanitized_sheetname}'!")

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Value Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()


# Example usage
# if __name__ == "__main__":
#     # Specify the path to your JSON file
#     file_path = "/home/devesh/Desktop/backend/artifact/Test_Data.json"
#     sheet_name = "2024 Feedback and Quotes"
    
#     # Open and load the JSON file
#     with open(file_path, "r") as file:
#         json_data = json.load(file)
    
#     json_data = json_data[:100]
#     # Measure time to create tables
#     start_time_create_tables = time.time()  # Start time for creating tables
#     create_tables_dynamically(json_data=json_data, sheetname=sheet_name)
#     end_time_create_tables = time.time()  # End time for creating tables
#     print(f"Time taken to create tables: {end_time_create_tables - start_time_create_tables:.2f} seconds")
    
#     # Measure time to insert data
#     start_time_insert_data = time.time()  # Start time for inserting data
#     insert_data(json_data=json_data, sheetname=sheet_name)
#     end_time_insert_data = time.time()  # End time for inserting data
#     print(f"Time taken to insert data: {end_time_insert_data - start_time_insert_data:.2f} seconds")
