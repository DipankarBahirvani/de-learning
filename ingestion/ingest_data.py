import argparse
import pandas as pd
import sqlalchemy as db
import os


def connect_to_db(args):
    host = args.host
    password = args.password
    username = args.username
    port = args.port
    db_name = args.database
    engine = db.create_engine(
        f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
    )
    return engine


def get_data(url):
    csv_name = url.split("/")[-1]
    print(csv_name)
    print("Downloading data")
    os.system(f"wget {url} -O test_data/{csv_name}")
    return csv_name


def write_data(engine, csv_name):
    df_iter = pd.read_csv(f"test_data/{csv_name}", chunksize=10000, iterator=True)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name="yellow_taxi_table", con=engine, if_exists="replace")
    for data in df_iter:
        data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"])
        data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"])
        data.to_sql(name="yellow_taxi_table", con=engine, if_exists="append")


def write_zonal_data(engine, csv_name):
    df_iter = pd.read_csv(f"test_data/{csv_name}", chunksize=100, iterator=True)
    # df = next(df_iter)

    # df.head(n=0).to_sql(name="zones", con=engine, if_exists="replace")
    for data in df_iter:
        data.to_sql(name="zones", con=engine, if_exists="append")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Intgest Data to DB")
    parser.add_argument("--host", type=str, help="host of database")
    parser.add_argument("--port", type=int, help="port number of database")

    parser.add_argument("--username", type=str, help="username of database")

    parser.add_argument("--password", type=str, help="password of database")
    parser.add_argument("--database", type=str, help="name of database")

    parser.add_argument("--url", type=str, help="url of data souce")

    parser.add_argument("--url1", type=str, help="url of data souce")

    args = parser.parse_args()
    engine = connect_to_db(args)
    if args.url:
        csv_name = get_data(args.url)
        write_data(engine, csv_name)
    if args.url1:
        csv_name = get_data(args.url1)
        write_zonal_data(engine, csv_name)
