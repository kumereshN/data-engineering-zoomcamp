import pandas as pd
from sqlalchemy import create_engine
import os
import argparse
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'yellow_tripdata_2021-01.csv.gz'

    os.system(f"wget {url} -O {csv_name}")
    # Download the csv 
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # # Break down the csv file into 100_000 rows per chunk
    # easier to insert the data in to the database
    df_iter = pd.read_csv(f'{csv_name}', iterator=True, chunksize=100000)

    df = next(df_iter)

    # ## Convert to `datime` format
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # df.head().to_sql(name = table_name, con = engine, if_exists = 'replace')
    df.to_sql(name=table_name, con=engine, if_exists='replace')


    # # Insert the chunks into the postgresql database
    while True:
        t_start = time()
        df = next(df_iter)
        
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        
        df.to_sql(name = table_name, con = engine, if_exists = 'append')
        
        t_end = time()

        print('insert another chunk ..., took %.3f second' % (t_end -t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, 
    # password,
    # host,
    # port,
    # database name
    # table name
    # url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)