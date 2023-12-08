import re
import ast

import pandas as pd
import numpy as np


class PipelineTransformations:

    @staticmethod
    def dates_standardize(hotel_bookings_df) -> pd.DataFrame:
        hotel_bookings_df['arrival_date'] = pd.to_datetime(hotel_bookings_df['arrival_date_year'].astype(str) + '-' +
                                                           hotel_bookings_df['arrival_date_month'] + '-' +
                                                           hotel_bookings_df['arrival_date_day_of_month'].astype(str),
                                                           errors='coerce')
        columns_to_drop = ['arrival_date_year', 'arrival_date_month', 'arrival_date_day_of_month']
        hotel_bookings_df.drop(columns=columns_to_drop, inplace=True)
        hotel_bookings_df['reservation_status_date'] = pd.to_datetime(hotel_bookings_df['reservation_status_date'],
                                                                      format='%d/%m/%Y', errors='coerce')

        return hotel_bookings_df

    @staticmethod
    def agents_imputation(hotel_bookings_df) -> pd.DataFrame:
        # Calculate the distribution of the 'agent' values (excluding NaN)
        agent_distribution = hotel_bookings_df['agent'].value_counts(normalize=True)

        # Generate random samples based on the distribution
        missing_indices = hotel_bookings_df['agent'].isnull()
        num_missing = missing_indices.sum()

        random_agents = np.random.choice(agent_distribution.index, size=num_missing, p=agent_distribution.values)

        # Fill in missing 'agent' values with the randomly sampled values
        hotel_bookings_df.loc[missing_indices, 'agent'] = random_agents
        return hotel_bookings_df

    @staticmethod
    def countries_imputation(hotel_bookings_df) -> pd.DataFrame:
        missing_country_rows = hotel_bookings_df[
            (hotel_bookings_df['country'] == 'Unknown') & (~hotel_bookings_df['agent'].isnull())]
        # For each such row, fill in 'country' based on the most common 'country' for that 'agent'
        for index, row in missing_country_rows.iterrows():
            agent = row['agent']
            possible_countries = hotel_bookings_df[hotel_bookings_df['agent'] == agent]['country']
            if not possible_countries.empty:
                most_common_country = possible_countries.mode().iloc[0]
                hotel_bookings_df.at[index, 'country'] = most_common_country
        return hotel_bookings_df

    @staticmethod
    def get_address_subfields(users_df) -> pd.DataFrame:
        # Address subfields
        users_df['address'] = users_df['address'].apply(ast.literal_eval)
        users_df['street'] = users_df['address'].apply(lambda x: x['street'])
        users_df['suite'] = users_df['address'].apply(lambda x: x['suite'])
        users_df['city'] = users_df['address'].apply(lambda x: x['city'])
        users_df['zipcode'] = users_df['address'].apply(lambda x: x['zipcode'])
        users_df['geo_lat'] = users_df['address'].apply(lambda x: x['geo']['lat'])
        users_df['geo_lng'] = users_df['address'].apply(lambda x: x['geo']['lng'])
        return users_df

    @staticmethod
    def get_company_subfields(users_df) -> pd.DataFrame:
        users_df['company'] = users_df['company'].apply(ast.literal_eval)
        users_df['company_name'] = users_df['company'].apply(lambda x: x['name'])
        users_df['company_catchPhrase'] = users_df['company'].apply(lambda x: x['catchPhrase'])
        users_df['company_bs'] = users_df['company'].apply(lambda x: x['bs'])
        return users_df

    @staticmethod
    def standardize_phone(phone) -> str:
        # Remove non-numeric characters
        phone = re.sub(r'\D', '', str(phone))

        # Remove common prefixes
        phone = re.sub(r'^\+1', '', phone)  # Remove '+1' prefix

        # Remove 'x' and anything following it
        phone = re.sub(r'x.*$', '', phone, flags=re.IGNORECASE)

        # Ensure the phone number starts with the country code
        if not phone.startswith(('+')):
            phone = '+1' + phone  # Assuming US country code

        return phone

    @staticmethod
    def is_valid_email(email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
