import os
import random
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils import save_json , remove_stopwords, extract_keywords

from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass


@dataclass
class keyword_exctractionConfig:
    keyword_file_path = os.path.join("artifact","Test_Data.json")
    
    
class Extract_clean_json:
    def __init__(self):
        self.keyword_exctraction_config = keyword_exctractionConfig()
        
        
    def initiate_json(self,data_path,target_column):
        try:
            logging.info("Read feedback data")

            # Load the dataset
            df = pd.read_excel(data_path) 
            logging.info("Loaded cleaned dataset")

            # Remove stopwords and save results
            df[["Clean_Text", 'stopwords']] = df[target_column].apply(lambda x: pd.Series(remove_stopwords(x)))

            # Extract keywords and save results
            df['Tags'] = df["Clean_Text"].apply(lambda x: extract_keywords(x, top_n=10))
            logging.info("Processed keywords and stopwords")

            # Convert stopwords list to comma-separated string
            df['stopwords'] = df['stopwords'].apply(lambda x: ', '.join(x))

            # Initialize list to store all processed data
            all_data = []

            # Process the first 5 rows
            for index, row in df.iterrows():
                row_data = {col: row[col] for col in df.columns}  # Create a dictionary for the current row
                
                # Append the row data to the all_data list
                all_data.append(row_data)
                logging.info(f"Processed row {index + 1} and appended to all_data")

            # Logging final success message
            logging.info("Finished processing all data")

                    
            save_json(all_data,file_path =self.keyword_exctraction_config.keyword_file_path)   
            
            return self.keyword_exctraction_config.keyword_file_path    
         
        except Exception as e:
            logging.error(e,"Extract_keyword error")
            raise CustomException(e, sys)            