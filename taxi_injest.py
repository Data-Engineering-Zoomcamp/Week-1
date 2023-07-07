import numpy as np
import pandas as pd
from time import time
from sqlalchemy import create_engine

df = pd.read_csv("data.csv", nrows=100).drop("Unnamed: 0", axis=1)

df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
print(pd.io.sql.get_schema(df, name="yellow"))
df.head()

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
engine.connect()

df_iter = pd.read_csv("data.csv", iterator=True, chunksize=100000)

for idx, chunk in enumerate(df_iter):
    t_start = time()

    df = chunk.drop("Unnamed: 0", axis=1)

    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["store_and_fwd_flag"] = np.where(df["store_and_fwd_flag"] == "Y", True, False)

    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

    t_end = time()

    print(f"inserted chunk {idx} in {t_end - t_start} seconds")
