import time
import pandas as pd 
from sqlalchemy import create_engine
import argparse
import requests


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    try :
        enginer = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        enginer.connect()
    except Exception as e:
        print(e)
        return
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    req = requests.get(url, allow_redirects=True)
    open(csv_name,'wb').write(req.content)

    df = pd.read_csv(csv_name,parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000,parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])

    df.head(0).to_sql(name=table_name,con=enginer)
    
    while 1:
        try :
            start_time = time.time()
            df_temp = next(df_iter)
            df_temp.to_sql(name=table_name,con=enginer,if_exists="append")
            end_time = time.time()
            print('inserted another chunk, took %.3f second' % (start_time - end_time))
        except Exception as e :
            print("The Input to Database Already Done")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument('--user', required=True, help='Username for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database for Postgres')
    parser.add_argument('--table_name', required=True, help='TableName for Postgres')
    parser.add_argument('--url', required=True, help='Data for inputing to Postgres')

    args = parser.parse_args()

    main(args)

