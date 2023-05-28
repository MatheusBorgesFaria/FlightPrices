import sys
from glob import glob
from os.path import join
from pprint import pprint
from time import time

from database_format import DatabaseFormat

sys.path.append("../odbc")
import query_tools as qt

sys.path.append("../utils")
from tools import get_relevant_path

data_upload = qt.get_table("data_upload")
files_already_computed = set(data_upload["filePath"].unique())


parquet_paths = glob(
    join(get_relevant_path("structured_data"), "*.parquet")
)

parquet_paths = list(set(parquet_paths) - files_already_computed)
parquet_paths.sort()

print("Parquets number: ", len(parquet_paths))
pprint(parquet_paths)

time_start = time()
database_format = DatabaseFormat(parquet_paths)
database_format.transform_all_parquets(n_jobs=-1, inset_on_database=True)
time_end = time()

print(f" Done in {(time_end - time_start)/60} min or equivalently {(time_end - time_start)/ (60 * 60)} hours")