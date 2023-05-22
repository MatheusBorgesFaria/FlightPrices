import pandas as pd

from connection import load_conn
from filter_warnings import filter_warnings


@filter_warnings
def insert_database(dataframe, table_name, schema="flight",
                    if_exists="append", chunksize=1_000_000,
                    method="multi"):
    """Insert dataframe on database.
    
    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe to insert on database.
    
    table_name: str
        The name of the database table.
    
    schema: str (default="flight")
        The name of the database schema.
    
    if_exists: str (default="fail")
        How to behave if the table already exists.
            fail: Raise a ValueError.
            replace: Drop the table before inserting new values.
            append: Insert new values to the existing table.
    
    chunksizeint: int (default=1_000_000)
        Specify the number of rows in each batch to be written at a time.

    method: str (default="multi")
        Controls the SQL insertion clause used:
            None : Uses standard SQL INSERT clause (one per row).
            multi: Pass multiple values in a single INSERT clause.
            callable with signature (pd_table, conn, keys, data_iter).
    """
    engine = load_conn(connection_type="engine")
    dataframe.to_sql(name=table_name, con=engine, schema=schema, 
                     if_exists=if_exists, chunksize=chunksize,
                     method=method, index=False) 