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
# Function to create the 'user' table
# def create_user_sheet_table():
#     conn = create_connection()
#     cursor = conn.cursor()

#     # SQL query to create the 'user' table
#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS user (
#         id INT AUTO_INCREMENT PRIMARY KEY,
#         sheetname VARCHAR(255) NOT NULL,
#         date DATE NOT NULL
#     );
#     """
#     cursor.execute(create_table_query)
#     conn.commit()

#     # Close connection
#     cursor.close()
#     conn.close()
#     print("User table created successfully.")

def create_user_sheet_table():
    conn = create_connection()
    cursor = conn.cursor()

    # Drop the table if it exists
    drop_table_query = """
    DROP TABLE IF EXISTS user;
    """
    cursor.execute(drop_table_query)

    # SQL query to create the 'user' table with additional columns and default values
    create_table_query = """
    CREATE TABLE Sheet_name (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sheetname VARCHAR(255) NOT NULL,
        date DATE NOT NULL,
        status ENUM('in_progress', 'completed') DEFAULT 'in_progress'
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Close connection
    cursor.close()
    conn.close()
    print("User table created successfully.")

# Function to insert data into the 'user' table
def insert_user_sheet_data(sheetname, date_value):
    conn = create_connection()
    cursor = conn.cursor()

    # SQL query to insert data into the 'user' table
    insert_query = """
    INSERT INTO Sheet_name (sheetname, date)
    VALUES (%s, %s);
    """
    
    # Execute the query with data
    cursor.execute(insert_query, (sheetname, date_value))
    conn.commit()

    # Close connection
    cursor.close()
    conn.close()
    print(f"Data for '{sheetname}' inserted successfully.")

    

def retrieve_user_data(sheetname):
    conn = create_connection()
    cursor = conn.cursor()

    # SQL query to check if a sheetname exists in the 'user' table
    select_query = "SELECT * FROM sheet_name WHERE sheetname = %s"
    cursor.execute(select_query, (sheetname,))

    # Fetch the result
    rows = cursor.fetchall()

    # If rows are found, sheetname exists in the table
    if rows:
        for row in rows:
            print(row)  # Optionally, print the row data
        # Return True if the sheetname is found
        cursor.close()
        conn.close()
        return False
    else:
        # Return False if the sheetname is not found
        cursor.close()
        conn.close()
        return True



def check_sheetname_exists(sheetname,date_value):
    # Create a connection to the database
    conn = create_connection()
    cursor = conn.cursor()

    # SQL query to check if a sheetname exists in the 'user' table
    select_query = "SELECT * FROM user WHERE sheetname = %s"
    cursor.execute(select_query, (sheetname,))

    # Fetch the result
    rows = cursor.fetchall()

    if rows:
        print(f"Sheet '{sheetname}' found in the table:")
        for row in rows:
            print(row)
    else:
        print(f"Sheet '{sheetname}' not found in the table.")
        result = insert_user_sheet_data(sheetname,date_value)

    # Close the connection
    cursor.close()
    conn.close()
    return result

# Example usage:
# if __name__ == "__main__":
#     create_user_sheet_table()  # Create the table (this will run once)
    # insert_user_sheet_data('2024 Feedback',date(2024,11,25))
#     # Insert some example data
#     insert_user_data('Sheet1', date(2024, 11, 25))
#     insert_user_data('Sheet2', date(2024, 11, 26))

#     # Retrieve and print data
#     print("\nData in 'user' table:")
#     retrieve_user_data()



# processing_data JSON DEFAULT NULL,
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP


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
                        date DATE NOT NULL,
                        status ENUM('in_progress', 'completed') DEFAULT 'in_progress',
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



