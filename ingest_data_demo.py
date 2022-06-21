import numpy as np
import os
import pandas as pd
from sqlalchemy import create_engine

username = 'root'
password = 'root'
host = 'localhost'
port = '3306'
dbname = 'taxi'

# Initialize mysql engine
engine = create_engine(
    f'mysql+pymysql://{username}:{password}@{host}:{port}/{dbname}')


def load_csv_to_db(csv_file, table_name):
    df = pd.read_csv(csv_file, index_col=0)

    # Create Table in taxi
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Load data into trip table
    df.to_sql(name=table_name, con=engine,
              if_exists='append', chunksize=100000)


def output_table_to_csv(query, csv_file):
    output_df = pd.read_sql(query, con=engine)
    output_df.to_csv(csv_file, index=None)


if __name__ == '__main__':
    # Create table
    load_csv_to_db('data/trip_100.csv', 'trip')

    # read from sql
    query = '''
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance
        FROM trip
        LIMIT 30;
    '''
    output_table_to_csv(query, 'sample_output.csv')
