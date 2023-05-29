from time import time

from file_transfer import structured_data_transfer


start_time = time()
structured_data_transfer()
end_time = time()

print(f"Done in {(end_time - start_time) / 60} min.")