from time import time
from database_tools import reindex


tables_list = ["search", "flight", "fare", "airport", "airline", "equipment", "data_upload"]
for table_name in tables_list:
    print(f"Reindex on table {table_name}")
    start_time = time()
    reindex(table_name)
    end_time = time()
    print(f"Done in {(end_time - start_time) / 60} min")
    