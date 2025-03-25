#load.py
import pandas as pd
import duckdb
from transform import join_data

import duckdb
from transform import join_data

def load_data_to_duckdb():
    # Define the DuckDB database file
    db_file = 'ms_county_stats.duckdb'

    # Get the transformed data
    df = join_data()

    if df is not None:
        try:
            # Connect to DuckDB and create a new table
            conn = duckdb.connect(db_file)
            conn.execute("CREATE OR REPLACE TABLE ms_county_stats AS SELECT * FROM df")
            print(f"Loaded data into {db_file}.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            conn.close()
    else:
        print("No data to load!")