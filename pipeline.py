from utils.data_driven_etl import DataDrivenETL

if __name__ == "__main__":
    data_driven_etl_instance = DataDrivenETL(data_source="CSV files")
    data_driven_etl_instance.extract()
    data_driven_etl_instance.transform(data=[])
    data_driven_etl_instance.load(transformed_data=[])
