import os
import re
import sys
import pandas as pd
from glob import glob
from pprint import pprint
from time import time

from database_format import DatabaseFormat

sys.path.append("../utils")
from tools import get_relevant_path

sys.path.append("../odbc")
import database_tools as dt
import query_tools as qt


def get_table_name(_str):
    """Extract from 'database_format_not_inserted' path the table name."""
    _str = re.search(r'(\w+)_\d+', _str).group(1)
    table_name = re.match(r"(\w+)_\d+_\d+h", _str).group(1)
    return table_name


parquet_paths = glob(
    os.path.join( 
        get_relevant_path("database_format_not_inserted"), "*.parquet"
    )
)

print("Parquets number: ", len(parquet_paths))
pprint(parquet_paths)

for parquet_path in parquet_paths:
    print(f"Path: {parquet_path}")

    time_begin = time()

    table_name = get_table_name(parquet_path)
    table = pd.read_parquet(parquet_path)

    database_format = DatabaseFormat(parquet_paths)

    if_exists = "append"
    if table_name in database_format.unique_value_tables:
        if_exists = "replace"
        current_table = qt.get_table(table_name)
        table = pd.concat([table, current_table])
        table = table.drop_duplicates(ignore_index=True)

    dataframe_not_inserted = dt.insert_database_parallel(table, table_name, if_exists=if_exists)
    database_format.save_dataframe_not_inserted(dataframe_not_inserted, table_name)

    os.remove(parquet_path)

    time_end = time()
    print(f"Total time: {(time_end - time_begin)/60} min table_name = {table_name}, shape = {table.shape} ") 