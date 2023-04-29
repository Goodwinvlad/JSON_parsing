import json
import pandas as pd
import datetime
from datetime import datetime as dt
from sqlalchemy import create_engine

import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

import os
from dotenv import load_dotenv


# get dataframe #1 with users
def get_df_1_users(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "userId",
        "platform",
        "durationMs",
        "position",
        "Date",
        "timestamp",
        "_owners",
        "owners_values",
        "resources",
    ]
    for index, row in df.iterrows():
        for elem in row["owners"]:

            key = elem
            values = row["owners"][elem]
            for value in values:
                df.at[index, "_owners"] = key
                df.at[index, "owners_values"] = value

    return df[columns]


# get dataframe #2 with content
def get_df_2_content(df: pd.DataFrame) -> pd.DataFrame:
    list_input = []
    columns = [
        "Date",
        "timestamp",
        "_owners",
        "owners_values",
        "resources",
        "resources_id",
    ]
    df = df[["Date", "timestamp", "_owners", "owners_values", "resources"]]
    for index, row in df.iterrows():
        for elem in row["resources"]:

            key = elem
            values = row["resources"][elem]
            for value in values:
                new_row = {
                    "Date": row["Date"],
                    "timestamp": row["timestamp"],
                    "_owners": row["_owners"],
                    "owners_values": row["owners_values"],
                    "resources": key,
                    "resources_id": value,
                }
                list_input.append(new_row)

                # df_input.loc[len(df_input.index)] = new_row
    return pd.DataFrame(list_input, columns=columns)


# convert timestamp to date and create add column "Date":
def timestamp_to_date(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = df["timestamp"].apply(
        lambda x: datetime.datetime.fromtimestamp(int(x) / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    df["Date"] = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M:%S").dt.date
    columns_df = [
        "userId",
        "platform",
        "durationMs",
        "position",
        "Date",
        "timestamp",
        "owners",
        "resources",
    ]
    df = df.reindex(columns=columns_df)
    return df


# get json file and transform into DataFrame:
file = "feeds_show.json"
data = [json.loads(line) for line in open(file, "r")]
df = pd.DataFrame(data)

# convert timestamp to date and create add column "Date":
df = timestamp_to_date(df)

# get dataframe #1 with users
start = dt.now()
df_1_users = get_df_1_users(df)
total_duration = dt.now() - start
print(f"Time of creating Table 'df_1_users' is {total_duration}")

# get dataframe #2 with content
start = dt.now()
df_2_content = get_df_2_content(df_1_users)
total_duration = dt.now() - start
print(f"Time of creating Table 'get_df_2_content' is {total_duration}")


# Connecting to PostgreSQL Database

# DEFINE THE DATABASE CREDENTIALS
load_dotenv()


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = int(os.getenv("POSTGRES_PORT"))
database = os.getenv("POSTGRES_DB")

engine = create_engine(
    url="postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)
)


# download postgresql table
data = df_1_users.iloc[:, :8]
data.to_sql("df_1_users", engine, if_exists="replace")

# download postgresql table
data = df_2_content
data.to_sql("df_2_content", engine, if_exists="replace")
