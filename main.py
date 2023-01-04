import argparse
import csv
import sqlite3
from sqlite3 import Error

import nfl_data_py as nfl
import pandas as pd

# Global Variables
SQLITE_DATABASE_URI = "sqlite/dbs/sqlite_db.db"
DROP_CREATE_SEASONAL_STATS_PATH = "sql/create_seasonal_stats_table.sql"
DROP_CREATE_PLAYER_IDS_PATH = "sql/create_player_ids_table.sql"
SEASONS = [2016, 2017, 2018, 2019, 2020, 2021, 2022]


def connect_to_db(file_path: str) -> sqlite3.Connection:
    """Connect to a SQLite database file"""
    connection = None
    try:
        connection = sqlite3.connect(file_path)
        return connection
    except Error as e:
        print(e)


def _execute_sql(sql_string: str, cursor: sqlite3.Cursor) -> None:
    """Execute a SQL query"""
    print(f"Executing the following SQL:\n{sql_string}")
    cursor.execute(sql_string)


def drop_and_create(path_to_sql_file: str, curr: sqlite3.Cursor) -> pd.DataFrame:
    with open(path_to_sql_file) as f:
        sql_statements = f.read().split("\n\n")
    _execute_sql(sql_string=sql_statements[0], cursor=curr)
    _execute_sql(sql_string=sql_statements[1], cursor=curr)


def fetch_seasonal_stats(seasons: list) -> pd.DataFrame:
    print(f"Retrieving seasonal stats for:")
    for season in seasons:
        print(season)
    df = nfl.import_seasonal_data(years=seasons, s_type="REG")
    return df


def fetch_player_ids() -> pd.DataFrame:
    print(f"Retrieving player ids")
    df = nfl.import_ids()
    return df


def main():
    # Argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--player_name", type=str, required=False)
    args = parser.parse_args()

    # Establish sqlite database connection
    conn = connect_to_db(file_path=SQLITE_DATABASE_URI)

    # Drop & create seasonal stats table
    drop_and_create(
        path_to_sql_file=DROP_CREATE_SEASONAL_STATS_PATH, curr=conn.cursor()
    )
    conn.commit()

    # Drop & create player ids table
    drop_and_create(path_to_sql_file=DROP_CREATE_PLAYER_IDS_PATH, curr=conn.cursor())
    conn.commit()

    # Fetch seasonal player stats
    seasonal_stats_df = fetch_seasonal_stats(seasons=SEASONS)
    seasonal_stats_df.to_sql(
        "seasonal_stats", con=conn, index=False, if_exists="append"
    )

    # Fetch player ids
    player_ids_df = fetch_player_ids()
    player_ids_df.to_sql("player_ids", con=conn, index=False, if_exists="append")

    # Lookup player
    if args.player_name:
        with open("sql/select_player_stats.sql") as f:
            sql = f.read()
        sql = sql + f"\nWHERE player_ids.\"name\" = '{args.player_name}';"
        cur = conn.cursor()
        data = cur.execute(sql)
        columns = [x[0] for x in data.description]

        rows = cur.fetchall()
        output_list = []
        for row in rows:
            output_list.append(dict(zip(columns, row)))

        # Write output csv
        with open(
            f"output_csvs/{output_list[0]['name']}.csv",
            "w",
            encoding="utf8",
            newline="",
        ) as output_file:
            writer = csv.DictWriter(output_file, fieldnames=output_list[0].keys())
            writer.writeheader()
            writer.writerows(output_list)

    conn.close()

if __name__ == "__main__":
    main()
