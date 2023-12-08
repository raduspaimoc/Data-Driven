import os
import logging
import requests

from utils.etl import ETL
from utils.pipeline_transformations import PipelineTransformations


import numpy as np
import pandas as pd


class DataDrivenETL(ETL):

    def __init__(self, logger: logging.Logger, files_separator: str = ETL.DEFAULT_SEPARATOR):
        super().__init__()
        self.logger = logger
        self.files_separator = files_separator

    def __get_data_from_files(self, files_to_extract: dict,  extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR):
        for save_file_name, extract_file_name in files_to_extract.items():
            self.logger.info(f"Started extraction: {extract_file_name}")
            df = pd.read_csv(extract_file_name, sep=self.files_separator)
            save_path = os.path.join(extractions_dir, save_file_name).replace("\\", "/")
            df.to_csv(save_path, index=False)
            self.logger.info(f"{save_file_name} extracted properly into {save_path}")

    def __get_data_from_apis(self, urls_to_extract: dict, extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR):
        for save_file_name, extract_url in urls_to_extract.items():
            self.logger.info(f"Started extraction: {extract_url}")
            response = requests.get(extract_url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Load JSON data into a Pandas DataFrame
                data = response.json()
                df = pd.DataFrame(data)
                save_path = os.path.join(extractions_dir, save_file_name).replace("\\", "/")
                df.to_csv(save_path, index=False)
                self.logger.info(f"{extract_url} extracted properly into {save_path}")
            else:
                self.logger.info(f"Error: {response.status_code} in {extract_url}")

    def extract(self, files_to_extract: dict, urls_to_extract: dict,
                extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR):

        self.__get_data_from_files(files_to_extract, extractions_dir=extractions_dir)
        self.__get_data_from_apis(urls_to_extract, extractions_dir=extractions_dir)

    def __bookings_transformations(self, hotel_bookings_df) -> pd.DataFrame:
        # Dates standarize
        hotel_bookings_df = PipelineTransformations.dates_standardize(hotel_bookings_df=hotel_bookings_df)
        # Data Cleansing
        hotel_bookings_df['meal'].replace({'Undefined': 'SC'}, inplace=True)
        hotel_bookings_df['country'].fillna('Unknown', inplace=True)
        # Data imputation
        hotel_bookings_df = PipelineTransformations.agents_imputation(hotel_bookings_df=hotel_bookings_df)
        hotel_bookings_df = PipelineTransformations.countries_imputation(hotel_bookings_df=hotel_bookings_df)
        return hotel_bookings_df

    def __users_transformations(self, users_df) -> pd.DataFrame:
        users_df = PipelineTransformations.get_address_subfields(users_df=users_df)
        users_df = PipelineTransformations.get_company_subfields(users_df=users_df)
        users_df.drop(['address', 'company'], axis=1, inplace=True)
        users_df['phone'] = users_df['phone'].apply(PipelineTransformations.standardize_phone)
        users_df['email_valid'] = users_df['email'].apply(PipelineTransformations.is_valid_email)
        users_df['website'] = users_df['website'].apply(
            lambda x: 'http://' + x if not x.startswith(('http://', 'https://')) else x)
        users_df['name'] = users_df['name'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        users_df['username'] = users_df['username'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        users_df[['geo_lat', 'geo_lng']] = users_df[['geo_lat', 'geo_lng']].apply(pd.to_numeric, errors='coerce')
        return users_df

    def transform(self, data, extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR):
        hotel_bookings_file_name = data[0]
        users_file_name = data[1]

        hotel_bookings_df = pd.read_csv(os.path.join(extractions_dir, hotel_bookings_file_name).replace("\\", "/"))
        users_df = pd.read_csv(os.path.join(extractions_dir, users_file_name).replace("\\", "/"))

        hotel_bookings_df = self.__bookings_transformations(hotel_bookings_df=hotel_bookings_df)
        hotel_bookings_df.drop_duplicates(inplace=True)

        users_df = self.__users_transformations(users_df=users_df)


        print("Applying data-driven transformations...")

    def load(self, transformed_data):
        print("Loading data into a data-driven storage...")

    def additional_functionality(self):
        print("Adding additional functionality specific to DataDrivenETL...")
