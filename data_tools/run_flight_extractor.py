import re
from datetime import datetime, timedelta
from glob import glob
from os.path import join

import pandas as pd
from flight_extractor import FlightExtractor
from tqdm import tqdm

start_date = (datetime.now() - timedelta(days=1)).date()
end_date = start_date
# start_date = datetime.strptime("2023-05-05", "%Y-%m-%d").date()
# end_date = datetime.now().date()

path_to_save = "/home/mborges/structured_data"
days_path_list = glob('/home/mborges/data/*')
overwrite_files = False


if not overwrite_files:
    structured_data_list = glob(join(path_to_save, "*.parquet"))
    computed_days = [
        re.findall(r"(\d{4}-\d{2}-\d{2})_structured_data", path)[0]
        for path in structured_data_list
    ]

for day_path in tqdm(days_path_list):
    day_str = re.findall(r"today_(\d{4}-\d{2}-\d{2})", day_path)[0]
    day = datetime.strptime(day_str, "%Y-%m-%d").date()
    if ((day < start_date or end_date < day) or
        (not overwrite_files and day_str in computed_days)):
        continue
    filenames_all = glob(join(day_path, "*/*/*.json"), recursive = True)
    if len(filenames_all) > 0:
        print(f"Structure data of the day {day_str}")

        extractor = FlightExtractor(filenames_all)
        structured_data, error_log_df = extractor.structure_all_jsons(n_jobs=-1)

        structured_data_path = join(path_to_save, day_str + "_structured_data.parquet")
        structured_data.to_parquet(structured_data_path)
        if not error_log_df.empty:
            error_log_path = join(path_to_save, "logs", day_str + "_error_log.csv")
            error_log_df.to_csv(error_log_path, index=False)
        del structured_data
        del error_log_df
