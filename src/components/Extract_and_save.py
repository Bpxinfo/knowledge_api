import os
import sys
import json
import asyncio
import logging
import aiohttp


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
from src.utils import extract_medical_lists, sanitize_column_name
from src.model.new_prompt import system_prompt

load_dotenv()
KEY = os.getenv("key")
ENDPOINT = os.getenv("endpoint")

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
                return None
            
            return extracted_data
        except Exception as e:
            logging.error(f"LLM response error: {e}")
            # Return empty dict instead of raising exception
            return None
        
   

    async def extract_record(self, record: dict, target_column: str, system_prompt: str):
        try:
            target_column = sanitize_column_name(target_column)
            text = record.get(target_column, "")
            
            # Log the text being processed
            logging.info(f"Processing text: {text}")
            
            # Ensure text is not empty
            if not text:
                logging.warning(f"Empty text for record: {record}")
                return record
            
            extracted_data = await self.get_llm_response(system_prompt, text)
            
            # Check if LLM returned data
            if not extracted_data:
                logging.error(f"No response from LLM for record: {record}")
                return None

            # Merge original record with extracted data
            updated_record = record.copy()

            # Only update if extracted_data is not empty
            if extracted_data:
                updated_record.update(extracted_data)
            
            
            return updated_record
        
        except Exception as e:
            # logging.error(f"Error in record extraction or database insert: {e}")
            # return record # If we don't want empty list of extracted record remove return
            # error_message = f"Error in record extraction: {str(e)}"
            logging.error(f"Error in record extraction: {str(e)}")
            return None

    async def load_and_extract(self, data_path: str, target_column: str, batch_size: int = 50):
        try:
            # Read input data
            with open(data_path, 'r', encoding='utf-8') as json_file:
                data_list = json.load(json_file)
            
            data_list = data_list[:10]
            total_records = len(data_list)
            logging.info(f"Loaded {total_records} records")

            # Handle case when batch_size is greater than total records
            batch_size = min(batch_size, total_records)

            # Initialize list to store all processed records
            all_processed_records = []
            total_successful = 0
            total_failed = 0
            failed_records = []

            for i in range(0, total_records, batch_size):
                # Get the current batch of records
                batch = data_list[i:i+batch_size]
                
                logging.info(f"Processing batch {i//batch_size + 1}: {len(batch)} records")

                # Process records in the current batch concurrently
                tasks = [
                    self.extract_record(record, target_column, system_prompt) 
                    for record in batch
                ]

                # Use return_exceptions to handle individual task failures
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process batch results
                for original_record, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        failed_records.append({
                            'record': original_record,
                            'error': str(result)
                        })
                        total_failed += 1
                        logging.error(f"Failed to extract record: {result}")
                    else:
                        # Successful extraction
                        all_processed_records.append(result)
                        total_successful += 1

                # Optional: Add a small delay between batches to avoid rate limiting
                await asyncio.sleep(1)

            # Save processed records to JSON
            with open(self.relation_file_path, 'w', encoding='utf-8') as f:
                json.dump(all_processed_records, f, indent=2)

            # Prepare results dictionary
            results = {
                'total_records': total_records,
                'processed_records': all_processed_records,
                'failed_records': failed_records,
                'total_successful': total_successful,
                'total_failed': total_failed,
                'output_file': self.relation_file_path
            }

            logging.info(f"Batch extraction complete. "
                        f"Total Records: {total_records}, "
                        f"Successful: {total_successful}, "
                        f"Failed: {total_failed}")

            return self.relation_file_path

        except Exception as e:
            logging.error(f"Load and extract error: {e}")
            raise

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