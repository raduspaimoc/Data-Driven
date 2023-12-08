from utils.data_driven_etl import DataDrivenETL

if __name__ == "__main__":
    data_driven_etl_instance = DataDrivenETL()
    dfs = data_driven_etl_instance.extract({"hotel_bookings.csv": "reservasHotel.csv"}, {"users.csv": "https://jsonplaceholder.typicode.com/users"})
    # data_driven_etl_instance.transform(data=[])
    # data_driven_etl_instance.load(transformed_data=[])
