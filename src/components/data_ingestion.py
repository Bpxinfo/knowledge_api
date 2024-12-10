import os
import sys
# Add 'src' directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.exception import CustomException
from src.logger import logging
# from src.components.data_transformation import DataTransformation
# from src.components.data_transformation import DataTransformationConfig
# from src.components.Extract_tag import Tag_extraction
# from src.components.Extract_Relation import Relation_extraction
import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    feedback_data_path: str = os.path.join("artifact","data.xlsx")
    # State_data_path:str = os.path.join("artifact","State_data.xlsx")

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self,data, sheetname):
        logging.info("Enter the data ingestion method or component")
        try:
            df = pd.read_excel(data,sheet_name=sheetname)   
            # state_df = pd.read_excel(r"/home/devesh/Desktop/backend/data/Stakeholder Feedback, Quotes & 5 KBQ_Cleaned_Tagged_Relationship.xlsx",sheet_name='States', skiprows=1)
            logging.info("Read the dataset as dataframe")

            os.makedirs(os.path.dirname(self.ingestion_config.feedback_data_path),exist_ok=True)

            logging.info("Save Data")
            df.to_excel(self.ingestion_config.feedback_data_path,index=False,)
            # state_df.to_excel(self.ingestion_config.State_data_path,index=False,header=True)

            logging.info("Ingestion is done")

            return self.ingestion_config.feedback_data_path
                # self.ingestion_config.State_data_path
        except Exception as e:
            raise CustomException(e,sys)
             
# if __name__=="__main__":
#     obj = DataIngestion()
    # feedback_path, state_path = obj.initiate_data_ingestion()
    
    # data_transformation = DataTransformation()
    # arr,Transforme_data_path = data_transformation.initiate_Data_transformation(feedback_path,state_path)
    
    # tag_json = Tag_extraction()
    # tag_json_path = tag_json.initiate_tag_extraction(data_path=Transforme_data_path)
    # # tag_json_path = r"/home/devesh/Desktop/backend/src/artifact/Test_Data.json"

    # Relation_json = Relation_extraction()
    # Relation_json.initiate_relation_extraction(data_path=tag_json_path)