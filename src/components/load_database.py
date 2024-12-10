import os
import sys
import json
import asyncio
import logging
import aiohttp
import aiomysql
import pandas as pd
from dotenv import load_dotenv

# Ensure these are imported correctly from your project structure
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.exception import CustomException
from src.logger import logging
from src.db.insert_create_table import check_table_exists, create_tables_dynamically
from src.db.insert_create_table import get_sql_type,sanitize_sheetname
# Load environment variables
load_dotenv()


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class load_into_db:
    # def __init__(self):


    # The current method looks incomplete
    async def create_db_connection(self):
        """Async method to create database connection pool"""
        try:
            pool = await aiomysql.create_pool(
                host=os.getenv('host'),
                port=int(os.getenv('DB_PORT', 3306)),
                user=os.getenv('user'),
                password=os.getenv('password'),
                db=os.getenv('database'),
                autocommit=True
            )
            return pool
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            raise   

    async def insert_sheet_name_once(self, sheetname):
        """
        Insert sheet name only if it doesn't already exist
        Returns the sheet_name_id
        """
        try:
            pool = await self.create_db_connection()
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # First, try to get existing sheet name ID
                    check_query = """
                    SELECT id FROM Sheet_name 
                    WHERE sheetname = %s
                    """
                    await cursor.execute(check_query, (sheetname,))
                    existing_sheet = await cursor.fetchone()
                    
                    if existing_sheet:
                        # Sheet name already exists, return its ID
                        return existing_sheet[0]
                    
                    # If not exists, insert new sheet name
                    insert_query = """
                    INSERT INTO Sheet_name (sheetname, date, status) 
                    VALUES (%s, CURRENT_DATE, 'in_progress')
                    """
                    await cursor.execute(insert_query, (sheetname,))
                    
                    # Get the ID of the newly inserted sheet name
                    sheet_name_id = cursor.lastrowid
                    
                    await conn.commit()
                    return sheet_name_id

        except Exception as e:
            logging.error(f"Error inserting sheet name: {e}")
            raise        
    
    async def row_insert_data(self, json_data,sheet_name_id, sanitized_sheetname):
        """Insert data into dynamically created tables, including Sheet_name table"""
        try:
            # Use async database connection
            pool = await self.create_db_connection()
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    
                    # sanitized_sheetname = sanitize_sheetname(sheetname)
                    Table_name = f"`{sanitized_sheetname}`"
                    nodes = f"`{sanitized_sheetname}_nodes`"
                    edges = f"`{sanitized_sheetname}_edges`"

                    # Ensure json_data is a list of dictionaries
                    if isinstance(json_data, str):
                        json_data = json.loads(json_data)

                    if isinstance(json_data, dict):
                        json_data = [json_data]

                    for record in json_data:
                        try:
                            if not isinstance(record, dict):
                                logging.warning(f"Skipping non-dictionary record: {record}")
                                continue

                            # Process feedback data, excluding nested keys
                            feedback_data = {
                                k: v for k, v in record.items() 
                                if k not in ['nodes', 'edges', 'Theme', 'Safety', 'Treatment', 
                                            'Diagnosis', 'sentiment', 'AnalyzeThoroughly', 'Issue', 
                                            'keywords', 'stopwords', 'synonyms']
                            }

                            # Add sheet_name_id to feedback_data
                            feedback_data['sheet_name_id'] = sheet_name_id

                            # Convert lists to JSON strings for fields
                            list_fields = ['Theme', 'Safety', 'Treatment', 'Diagnosis', 
                                        'sentiment', 'AnalyzeThoroughly', 'keywords', 'synonyms', 'Tags']
                            for field in list_fields:
                                feedback_data[field] = json.dumps(record.get(field, [])) if record.get(field) else None

                            feedback_data['Issue'] = record.get('Issue')
                            feedback_data['Stopwords'] = record.get('stopwords')

                            # Construct and execute the insert query for feedback data
                            columns = ', '.join(f"`{k}`" for k in feedback_data.keys())
                            placeholders = ', '.join(['%s'] * len(feedback_data))
                            insert_feedback_query = f"INSERT INTO {Table_name} ({columns}) VALUES ({placeholders})"
                            
                            await cursor.execute(insert_feedback_query, list(feedback_data.values()))
                            feedback_id = cursor.lastrowid

                            # Insert nodes if available
                            if 'nodes' in record:
                                node_queries = []
                                node_params = []
                                for node in record['nodes']:
                                    node_queries.append(f"""
                                    INSERT INTO {nodes} (feedback_id, node_id, label, type)
                                    VALUES (%s, %s, %s, %s)
                                    """)
                                    node_params.append((feedback_id, node['id'], node['label'], node['type']))
                                
                                await cursor.executemany(node_queries[0], node_params)

                            # Insert edges if available
                            if 'edges' in record:
                                edge_queries = []
                                edge_params = []
                                for edge in record['edges']:
                                    edge_queries.append(f"""
                                    INSERT INTO {edges} (feedback_id, source, target, relationship)
                                    VALUES (%s, %s, %s, %s)
                                    """)
                                    edge_params.append((feedback_id, edge['source'], edge['target'], edge['relationship']))
                                
                                await cursor.executemany(edge_queries[0], edge_params)

                        except Exception as record_error:
                            # Log the error for the specific record but continue processing
                            logging.error(f"Error inserting record: {record_error}")
                            logging.warning(f"Continuing with next record after error in: {record}")
                            continue

                    # Update sheet_name status to completed after successful insertion
                    update_sheet_status_query = """
                    UPDATE Sheet_name 
                    SET status = 'completed' 
                    WHERE id = %s
                    """
                    await cursor.execute(update_sheet_status_query, (sheet_name_id,))

                    # Commit the transaction
                    await conn.commit()
                    logging.info(f"Data insertion completed for '{sanitized_sheetname}'!")



        except Exception as e:
            logging.error(f"Overall database insertion error: {e}")
            raise
    
        
    async def insert_record(self, record: dict, sheet_name_id:str,sanitized_sheetname: str):
        """
        Parallel insert method for database records
        """
        try:
            # Insert record
            await self.row_insert_data([record], sheet_name_id, sanitized_sheetname)
            
            logging.info(f"Successfully inserted record: {record.get('feedback_id', 'Unknown ID')}")
            return record
        except Exception as e:
            logging.error(f"Database insertion error: {e}")
            return None    



    async def load_and_database(self, data_path: str, sheet_name: str):
        try:
  
            # Read input data
            with open(data_path, 'r', encoding='utf-8') as json_file:
                data_list = json.load(json_file)
            
            logging.info(f"Loaded {len(data_list)} records")

            # Limit to first 2 records for demonstration (remove for full processing)
            processed_records = data_list[:100]

            # # # Process each record
            for record in processed_records:
                # Check and create table if not exists

                if not check_table_exists(sheetname=sheet_name):
                    create_tables_dynamically(json_data=record, sheetname=sheet_name)
            
            sheet_name_id = await self.insert_sheet_name_once(sheet_name)
            sanitized_sheetname = sanitize_sheetname(sheet_name)
                
            # Stage 2: Parallel Insertion
            insertion_tasks = [
                self.insert_record( record,sheet_name_id,sanitized_sheetname)
                for record in processed_records
            ]

            inserted_records = await asyncio.gather(*insertion_tasks)


            logging.info(f"Processed records saved into database")
            return processed_records

        except Exception as e:
            logging.error(f"Load and extract error: {e}")
            raise

# async def main():

#     # Configuration
#     data_path = r"C:\Users\nickc\OneDrive\Desktop\New folder (2)\Backend\artifact\Relation.json"
#     target_column = "Feedback/Quotes From Stakeholders"
#     sheet_name = "kitna bakbas he"

#     # Initialize and run extraction
#     extractor = load_into_db()
    
#     # Run extraction
#     processed_records = await extractor.load_and_database(
#         data_path=data_path, 
#         sheet_name=sheet_name,
#     )
    

# if __name__ == "__main__":
#     asyncio.run(main())