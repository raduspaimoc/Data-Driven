class ETL:
    DEFAULT_SEPARATOR = ";"
    DEFAULT_EXTRACTIONS_DIR = "data/extract"
    DEFAULT_TRANSFORMATIONS_DIR = "data/transform"

    def __init__(self):
        self.data = None

    def extract(self, files_to_extract: dict, urls_to_extract: dict, extractions_dir: str = DEFAULT_EXTRACTIONS_DIR):
        print(f"Extracting data...{self.data}")

    def transform(self, data):
        self.data = data
        print(f"Transforming data...{data}")

    def load(self, transformed_data):
        print(f"Loading data into a generic storage...{self.data} tranformed into {transformed_data}.")
