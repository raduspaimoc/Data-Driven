import logging
from logging import FileHandler


from utils.data_driven_etl import DataDrivenETL
from utils.general_functions import get_logger

if __name__ == "__main__":
    logger = get_logger()

    data_driven_etl_instance = DataDrivenETL(logger=logger)
    dfs = data_driven_etl_instance.extract({"hotel_bookings.csv": "reservasHotel.csv"}, {"users.csv": "https://jsonplaceholder.typicode.com/users"})
    # data_driven_etl_instance.transform(data=[])
    # data_driven_etl_instance.load(transformed_data=[])
