from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging import logger
from networksecurity.logging.logger import logging as Logger


from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import sys
import os   
import pandas as pd
import numpy as np
import pymongo
from sklearn.model_selection import train_test_split

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGODB_URL")

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            Logger.info(self.__class__.__name__, "Data Ingestion log started.")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e
        
    def export_collection_as_dataframe(self):
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.client = pymongo.MongoClient(MONGO_DB_URL)
            collection_name = self.client[database_name][collection_name]
            data = pd.DataFrame(list(collection_name.find()))
            if "_id" in data.columns:
                data = data.drop(columns=["_id"], axis=1)

            data.replace({"na":np.nan},inplace=True)
            return data
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e
        
    def export_data_into_feature_store(self, data: pd.DataFrame):
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            data.to_csv(feature_store_file_path, index=False, header=True)
            return data
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e 
        
    def train_test_split(self, data: pd.DataFrame):
        try:
            train_set, test_set = train_test_split(
                data, 
                test_size=self.data_ingestion_config.train_test_split_ratio
            )
            Logger.info("Performed train test split on the dataset")
            Logger.info(f"Train set length: {len(train_set)}")
            Logger.info(f"Test set length: {len(test_set)}")

            train_file_path = self.data_ingestion_config.training_file_path
            test_file_path = self.data_ingestion_config.testing_file_path
            train_dir_path = os.path.dirname(train_file_path)
            os.makedirs(train_dir_path, exist_ok=True)
            test_dir_path = os.path.dirname(test_file_path)
            os.makedirs(test_dir_path, exist_ok=True)
            train_set.to_csv(train_file_path, index=False, header=True)
            test_set.to_csv(test_file_path, index=False, header=True)
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e


        
    def initiate_data_ingestion(self):
        try:
            data=self.export_collection_as_dataframe()
            data=self.export_data_into_feature_store(data)
            self.train_test_split(data)
            Logger.info("Data Ingestion log completed.")

            data_ingestion_artifact = DataIngestionArtifact(
                training_file_path=self.data_ingestion_config.training_file_path,
                testing_file_path=self.data_ingestion_config.testing_file_path,
                feature_store_file_path=self.data_ingestion_config.feature_store_file_path
            )
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e