from utils.etl import ETL


class DataDrivenETL(ETL):
    def __init__(self, data_source):
        self.data_source = data_source

    def extract(self):
        print(f"Extracting data from {self.data_source}...")

    def transform(self, data):
        print("Applying data-driven transformations...")

    def load(self, transformed_data):
        print("Loading data into a data-driven storage...")

    def additional_functionality(self):
        print("Adding additional functionality specific to DataDrivenETL...")
