class ETL:
    def __init__(self):
        self.data = None

    def extract(self):
        print(f"Extracting data...{self.data}")

    def transform(self, data):
        self.data = data
        print(f"Transforming data...{data}")

    def load(self, transformed_data):
        print(f"Loading data into a generic storage...{self.data} tranformed into {transformed_data}.")
        