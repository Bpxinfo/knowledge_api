import mysql.connector
from datetime import date
from dotenv import load_dotenv
load_dotenv()
import os
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


def retrieve_user_data(sheetname):
    conn = create_connection()
    cursor = conn.cursor()

    # SQL query to check if a sheetname exists in the 'sheet_name' table
    select_query = "SELECT * FROM sheet_name WHERE sheetname = %s"
    cursor.execute(select_query, (sheetname,))

    # Fetch the result
    rows = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Return True if the sheetname is found, False otherwise
    return bool(rows)



def is_sheet_name_table_exists():
    """
    Check if the Sheet_name table exists in the database.
    
    Returns:
    - True if table exists
    - False if table does not exist
    """
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        # Query to check if table exists in the current database
        check_table_query = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
        AND table_name = 'Sheet_name'
        """
        
        cursor.execute(check_table_query)
        
        # Fetch the result
        table_exists = cursor.fetchone()[0] > 0
        
        return table_exists
    
    except mysql.connector.Error as err:
        print(f"Error checking table existence: {err}")
        return False
    finally:
        # Ensure connections are closed
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()


def create_sheet_name_table():
    """
    Create the Sheet_name table if it does not exist.
    
    Returns:
    - True if table was created
    - False if table already exists or creation failed
    """
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        # Create table query
        create_table_query = """
                    CREATE TABLE Sheet_name (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                sheetname VARCHAR(255) NOT NULL,
                                column_name VARCHAR(255),
                                metadata VARCHAR(255),
                                State VARCHAR(255),
                                Stakeholder VARCHAR(255),
                                col_Date VARCHAR(255),
                                Region VARCHAR(255),
                                date DATE NOT NULL,
                                status ENUM('in_progress', 'completed') DEFAULT 'in_progress'
                            );
                    """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        print("Sheet_name table created successfully.")
        return True
    
    except mysql.connector.Error as err:
        print(f"Error creating Sheet_name table: {err}")
        return False
    finally:
        # Ensure connections are closed
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()


