import sys
from dataclasses import dataclass
import os
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
# from sklearn.base import TransformerMixin, BaseEstimator 
from sklearn.compose import ColumnTransformer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils import  word_preprocess 
from src.exception import CustomException
from src.logger import logging

@dataclass
class DataTransformationConfig:
    # preprocessor_obj_file_path_pkl = os.path.join(r'C:\Users\abc\OneDrive\Desktop\back\src\artifact','process.pkl')
    preprocessor_obj_file_path_excel = os.path.join('artifact','transform.xlsx')
    

class DataTransformation:
    def __init__(self):
        logging.info("Start")
        self.data_transformation_config = DataTransformationConfig()
    def get_data_transformation_object(self,col_name):
        try:
            logging.info("pipeline process")
            # process_columns = col_name
            logging.info(col_name)
            # Define the pipeline with DropNullsTransformer and SimpleImputer
            pipeline = Pipeline(
                steps=[
                    # ("drop_nulls", DropNullsTransformer(columns=process_columns)),
                    ("imputer", SimpleImputer(strategy="most_frequent"))
                ]
            )
            
            logging.info("Data transformation pipeline created successfully.")

            preprocessor = ColumnTransformer(
                [
                    ("pipeline",pipeline,col_name)
                ]
            )
            return preprocessor
        
        except Exception as e:
            logging.error("Error in creating data transformation pipeline.")
            raise CustomException(e, sys)
    
    def initiate_Data_transformation(self,feedback_path,target_columns,columnsname):
        try:
            logging.info("enter")
            data = pd.read_excel(feedback_path)
            # state_df = pd.read_excel(State_path)

            logging.info("Read data and State data")
            logging.info("Obtaining preprocessing object")
            # logging.info(columnsname.dtype)
            
            
            target_column = str(target_columns)
            columnname = list(columnsname)
            data = data.dropna(subset=[target_column])
            data = data.dropna(axis=1, how='all')

            col_data = data.columns
            print(col_data)
            filtered_column_list = [col for col in columnname if col in col_data]

            print(filtered_column_list)
            logging.info(
                f"Applying preposcessing object on data"
            )

            # process_columns = columnsname
            preprocessing_obj = self.get_data_transformation_object(col_name=filtered_column_list)
            target_feature_train_df = preprocessing_obj.fit_transform(data)
        
            transformed_df = pd.DataFrame(target_feature_train_df,columns=filtered_column_list[:len(target_feature_train_df[0])])
            
            logging.info(f"Applying preposcessing object on data")

            # transformed_df['date'] = data['Date'].values
             
            artifact_file_path = self.data_transformation_config.preprocessor_obj_file_path_excel
            transformed_df.to_excel(artifact_file_path, index=False,columns=filtered_column_list)
            # train_arr = np.c_[
            #      np.array(target_feature_train_df)
            # ]
            
            return self.data_transformation_config.preprocessor_obj_file_path_excel

        except Exception as e:
            logging.error(e,"Error in Data loading")
            raise CustomException(e, sys)    