import os
import sys
import json
import asyncio
import logging
import aiohttp
import aiomysql

import pandas as pd
from dotenv import load_dotenv
from azure.ai.inference.aio import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.core.credentials import AzureKeyCredential
from dataclasses import dataclass

# Ensure these are imported correctly from your project structure
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..')))
from src.exception import CustomException
from src.logger import logging
from src.utils import extract_medical_lists
from src.db.insert_create_table import row_insert_data, check_table_exists, create_tables_dynamically
from src.model.new_prompt import system_prompt
from src.db.insert_create_table import get_sql_type,create_table,sanitize_sheetname
# Load environment variables
load_dotenv()
KEY = os.getenv("key")
ENDPOINT = os.getenv("endpoint")
# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Relation_extraction:
    def __init__(self):
        self.client = ChatCompletionsClient(
            endpoint=ENDPOINT, 
            credential=AzureKeyCredential(KEY)
        )

        self.relation_file_path = os.path.join("artifact", "Relation.json")


    async def get_llm_response(self, system_prompt: str, text: str):
        try:
            user_message = f"Context: Gives all details from Statement: {text}"
            async with ChatCompletionsClient(
            endpoint=os.getenv('endpoint'),
            credential=AzureKeyCredential(os.getenv('key'))
                ) as client:
                response = await client.complete(
                    messages=[
                        SystemMessage(content=system_prompt),
                        UserMessage(content=user_message),
                    ]
                )
            output_triple = response.choices[0].message.content
            
            # Add logging to debug extraction
            logging.info(f"Raw LLM response: {output_triple}")
            
            # Ensure extract_medical_lists handles potential None or empty responses
            extracted_data = extract_medical_lists(output_triple)
            
            # If extraction fails, return an empty dict
            if extracted_data is None:
                logging.warning("No data extracted. Returning empty dictionary.")
                return {}
            
            return extracted_data
        except Exception as e:
            logging.error(f"LLM response error: {e}")
            # Return empty dict instead of raising exception
            return {}
        
   

    async def extract_record(self, record: dict, target_column: str, system_prompt: str):
        try:
            text = record.get(target_column, "")
            
            # Log the text being processed
            logging.info(f"Processing text: {text}")
            
            # Ensure text is not empty
            if not text:
                logging.warning(f"Empty text for record: {record}")
                return record
            
            extracted_data = await self.get_llm_response(system_prompt, text)
            
            # Merge original record with extracted data
            updated_record = record.copy()
            
            # Only update if extracted_data is not empty
            if extracted_data:
                updated_record.update(extracted_data)

            return updated_record
        except Exception as e:
            logging.error(f"Error in record extraction or database insert: {e}")
            # Return original record if extraction fails
            return record

    async def load_and_extract(self, data_path: str, target_column: str):
        try:

            # Read input data
            with open(data_path, 'r', encoding='utf-8') as json_file:
                data_list = json.load(json_file)
            
            logging.info(f"Loaded {len(data_list)} records")

            # Limit to first 2 records for demonstration (remove for full processing)
            data_list = data_list[:10]

            # Process records concurrently
            tasks = [
                self.extract_record(record, target_column, system_prompt) 
                for record in data_list
            ]
            processed_records = await asyncio.gather(*tasks)

            # Save processed records to JSON
            with open(self.relation_file_path, 'w', encoding='utf-8') as f:
                json.dump(processed_records, f, indent=2)

            logging.info(f"Processed records saved to {self.relation_file_path}")
            return self.relation_file_path

        except Exception as e:
            logging.error(f"Load and extract error: {e}")
            raise


    # async def load_and_extract(self, data_path: str, target_column: str):
    #     try:
    #         # Read input data
    #         with open(data_path, 'r', encoding='utf-8') as json_file:
    #             data_list = json.load(json_file)
            
    #         logging.info(f"Loaded {len(data_list)} records")

    #         # Initialize list to store all processed records
    #         all_processed_records = []

    #         # Process records in batches of 50
    #         for i in range(0, len(data_list), 50):
    #             # Get the current batch of 50 records (or remaining records if less than 50)
    #             batch = data_list[i:i+50]
                
    #             logging.info(f"Processing batch {i//50 + 1}: {len(batch)} records")

    #             # Process records in the current batch concurrently
    #             tasks = [
    #                 self.extract_record(record, target_column, system_prompt) 
    #                 for record in batch
    #             ]
    #             processed_batch = await asyncio.gather(*tasks)

    #             # Extend the all_processed_records list
    #             all_processed_records.extend(processed_batch)

    #             # Optional: Add a small delay between batches to avoid rate limiting
    #             await asyncio.sleep(1)

    #         # Save processed records to JSON
    #         with open(self.relation_file_path, 'w', encoding='utf-8') as f:
    #             json.dump(all_processed_records, f, indent=2)

    #         logging.info(f"Processed {len(all_processed_records)} records saved to {self.relation_file_path}")
    #         return self.relation_file_path

    #     except Exception as e:
    #         logging.error(f"Load and extract error: {e}")
    #         raise    

# async def main():
#     # Get credentials from environment

#     if not KEY or not ENDPOINT:
#         logging.error("Missing Azure credentials. Please set 'key' and 'endpoint' in .env")
#         sys.exit(1)

#     # Configuration
#     data_path = r"C:\Users\nickc\OneDrive\Desktop\New folder (2)\Backend\artifact\Test_Data.json"
#     target_column = "Feedback/Quotes From Stakeholders"
#     sheet_name = "2024 Feedback and Quotes"

#     # Initialize and run extraction
#     extractor = Relation_extraction()
    
#     # Run extraction
#     processed_records = await extractor.load_and_extract(
#         data_path=data_path, 
#         target_column=target_column, 
#     )
    
#     # Optional: print processed records
#     print(json.dumps(processed_records, indent=2))

# if __name__ == "__main__":
#     asyncio.run(main())