import os
import requests

from utils.etl import ETL




import pandas as pd


class DataDrivenETL(ETL):

    def __init__(self, files_separator: str = ETL.DEFAULT_SEPARATOR):
        super().__init__()
        self.files_separator = files_separator

    def extract(self, files_to_extract: dict, urls_to_extract: dict,
                extractions_dir: str = ETL.DEFAULT_EXTRACTIONS_DIR):

        extracted_dfs = []
        for save_file_name, extract_file_name in files_to_extract.items():
            print(f"Started extraction: {extract_file_name}")
            df = pd.read_csv(extract_file_name, sep=self.files_separator)
            save_path = os.path.join(extractions_dir, save_file_name).replace("\\", "/")
            df.to_csv(save_path, index=False)
            extracted_dfs.append(df)
            print(f"{save_file_name} extracted properly into {save_path}")

        for save_file_name, extract_url in urls_to_extract.items():
            print(f"Started extraction: {extract_url}")
            response = requests.get(extract_url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Load JSON data into a Pandas DataFrame
                data = response.json()
                df = pd.DataFrame(data)
                save_path = os.path.join(extractions_dir, save_file_name).replace("\\", "/")
                df.to_csv(save_path, index=False)
                extracted_dfs.append(df)
                print(f"{extract_url} extracted properly into {save_path}")
            else:
                print(f"Error: {response.status_code} in {extract_url}")
        return extracted_dfs

    def transform(self, data):
        print("Applying data-driven transformations...")

    def load(self, transformed_data):
        print("Loading data into a data-driven storage...")

    def additional_functionality(self):
        print("Adding additional functionality specific to DataDrivenETL...")
