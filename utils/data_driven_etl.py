import os
import logging
import requests

from utils.etl import ETL
from utils.pipeline_transformations import PipelineTransformations


import numpy as np
import pandas as pd


class DataDrivenETL(ETL):

    DWH_TABLES_INFO = {
        "dim_companies": ['company_name', 'company_catchPhrase', 'company_bs'],
        "dim_hotels": ["hotel_id", "hotel"],
        "dim_meals": ["meal_id", "meal"],
        "dim_dates": ["arrival_date_id", "arrival_date"],
        "dim_users": ['id', 'name', 'username', 'email', 'phone', 'website', 'street', 'suite', 'city', 'zipcode', 'geo_lat', 'geo_lng', 'company_id'],
        "fact_bookings": ['hotel_id', 'agent_id', 'meal_id', 'is_canceled', 'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children', 'country', 'is_repeated_guest', 'previous_cancellations', 'previous_bookings_not_canceled', 'reserved_room_type', 'assigned_room_type', 'reservation_status', 'reservation_status_date', 'arrival_date_id']
    }

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

    def __save_transformations_data(self, hotel_bookings_df: pd.DataFrame,
                                    users_df: pd.DataFrame, transformations_dir: str):
        # Dim companies
        unique_companies = users_df[self.DWH_TABLES_INFO["dim_companies"]].drop_duplicates()
        # Create a mapping between unique companies and company IDs
        company_mapping = {tuple(row): idx + 1 for idx, row in enumerate(unique_companies.itertuples(index=False))}
        # Assign company IDs to the DataFrame
        users_df['company_id'] = users_df[self.DWH_TABLES_INFO["dim_companies"]].apply(
            lambda row: company_mapping[tuple(row)], axis=1)
        companies_df = users_df[["company_id"] + self.DWH_TABLES_INFO["dim_companies"]].copy()
        self.logger.info("Dim companies created properly")


        # Dim hotel
        hotel_columns = ['hotel']
        unique_hotels = hotel_bookings_df[hotel_columns].drop_duplicates()
        # Create a mapping between unique companies and company IDs
        hotel_mapping = {tuple(row): idx + 1 for idx, row in enumerate(unique_hotels.itertuples(index=False))}
        hotel_bookings_df['hotel_id'] = hotel_bookings_df[hotel_columns].apply(lambda row: hotel_mapping[tuple(row)],
                                                                               axis=1)
        hotels_df = hotel_bookings_df[self.DWH_TABLES_INFO["dim_hotels"]].drop_duplicates().copy()
        self.logger.info("Dim hotels created properly")

        # Dim meals
        meal_columns = ['meal']
        unique_meal = hotel_bookings_df[meal_columns].drop_duplicates()
        # Create a mapping between unique companies and company IDs
        meal_mapping = {tuple(row): idx + 1 for idx, row in enumerate(unique_meal.itertuples(index=False))}
        hotel_bookings_df['meal_id'] = hotel_bookings_df[meal_columns].apply(lambda row: meal_mapping[tuple(row)],
                                                                             axis=1)
        meals_df = hotel_bookings_df[self.DWH_TABLES_INFO["dim_meals"]].drop_duplicates().copy()
        self.logger.info("Dim meals created properly")

        # Dim dates
        hotel_bookings_df['arrival_date_id'] = hotel_bookings_df['arrival_date'].dt.strftime('%Y%m%d').astype(int)
        dates_df = hotel_bookings_df[self.DWH_TABLES_INFO["dim_dates"]].drop_duplicates().copy()
        hotel_bookings_df.rename(columns={"agent": "agent_id"}, inplace=True)
        self.logger.info("Dim dates created properly")

        # Fact bookings
        unique_hotels = hotel_bookings_df[self.DWH_TABLES_INFO["fact_bookings"]].drop_duplicates()
        # Create a mapping between unique companies and company IDs
        hotel_mapping = {tuple(row): idx + 1 for idx, row in enumerate(unique_hotels.itertuples(index=False))}
        hotel_bookings_df['booking_id'] = hotel_bookings_df[self.DWH_TABLES_INFO["fact_bookings"]].apply(lambda row: hotel_mapping[tuple(row)], axis=1)
        fact_bookings_df = hotel_bookings_df[["booking_id"] + self.DWH_TABLES_INFO["fact_bookings"]].drop_duplicates()
        self.logger.info("Fact bookings created properly")

        # Load data
        companies_df.to_csv(os.path.join(transformations_dir, "dim_companies.csv").replace("\\"
                                                                                           "", "/"), index=False)
        users_df[self.DWH_TABLES_INFO["dim_users"]].to_csv(
            os.path.join(transformations_dir, "dim_users.csv").replace("\\", "/"), index=False)

        hotels_df.to_csv(os.path.join(transformations_dir, "dim_hotels.csv").replace("\\", "/"), index=False)
        meals_df.to_csv(os.path.join(transformations_dir, "dim_meals.csv").replace("\\", "/"), index=False)
        dates_df.to_csv(os.path.join(transformations_dir, "dim_dates.csv").replace("\\", "/"), index=False)
        fact_bookings_df.to_csv(os.path.join(transformations_dir, "fact_bookings.csv").replace("\\", "/"), index=False)
        self.logger.info("All tables data stored properly in trnaform dir")

    def transform(self, data, extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR,
                  transformations_dir: str = ETL.DEFAULT_TRANSFORMATIONS_DIR):
        hotel_bookings_file_name = data[0]
        users_file_name = data[1]

        hotel_bookings_df = pd.read_csv(os.path.join(extractions_dir, hotel_bookings_file_name).replace("\\", "/"))
        users_df = pd.read_csv(os.path.join(extractions_dir, users_file_name).replace("\\", "/"))

        hotel_bookings_df = self.__bookings_transformations(hotel_bookings_df=hotel_bookings_df)
        hotel_bookings_df.drop_duplicates(inplace=True)
        self.logger.info("Bookings transformations applied")

        users_df = self.__users_transformations(users_df=users_df)
        self.logger.info("Users transformations applied")

        self.__save_transformations_data(hotel_bookings_df, users_df, transformations_dir)
        self.logger.info("Tranform step finished properly")

    def load(self, transformed_data):
        print("Loading data into a data-driven storage...")

    def additional_functionality(self):
        print("Adding additional functionality specific to DataDrivenETL...")
