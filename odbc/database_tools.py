import pandas as pd
import numpy as np
from os import cpu_count
from joblib import Parallel, delayed

from connection import load_conn
from filter_warnings import filter_warnings


@filter_warnings
def insert_database_parallel(dataframe, table_name, schema="flight",
                             if_exists="append", chunksize=20_000,
                             method="multi", n_jobs=None,
                             n_dataframe_divisions=None, 
                             max_n_attempts=5):
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
    n_jobs: int (default=max(cpu_count() // 2, 1))
        Number of cores to insert data at the same time.
    n_dataframe_divisions: int (default=None)
        Number of times the dataframe is divided,
        each partition will be computed by a "processes".
        If it is None, n_dataframe_divisions = len(dataframe) // chunksize
    max_n_attempts: int (default=5)
        Maximum number of attempts to insert data into the database.
    
    Return
    ------
    dataframe_not_inserted: pd.DataFrame
        Dataframe that could not be inserted into the database.
    """
    if n_jobs is None:
        n_jobs = max(cpu_count() // 2, 1)
    assert n_jobs >= -1 and n_jobs != 0, "n_jobs must be a positive number or equal -1."
    
    if n_dataframe_divisions is None:
        n_dataframe_divisions = max(len(dataframe) // chunksize, 1)
    
    dataframe_not_inserted_list = Parallel(n_jobs=n_jobs, prefer="processes", verbose=1)(
        [delayed(insert_database)(
            dataframe=dataframe_part,
            table_name=table_name,
            schema=schema, 
            if_exists=if_exists,
            chunksize=chunksize,
            method=method,
            max_n_attempts=max_n_attempts
         )
         for dataframe_part in np.array_split(dataframe, n_dataframe_divisions)
        ]
    )
    
    dataframe_not_inserted = pd.concat(dataframe_not_inserted_list)
    return dataframe_not_inserted


@filter_warnings
def insert_database(dataframe, table_name, schema="flight",
                    if_exists="append", chunksize=20_000,
                    method="multi", max_n_attempts=5):
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
     max_n_attempts: int (default=5)
        Maximum number of attempts to insert data into the database.
    
    Return
    ------
    dataframe_not_inserted: pd.DataFrame
        Dataframe that could not be inserted into the database.
    """
    counter = 1
    while max_n_attempts >= counter:
        try:
            engine = load_conn(connection_type="engine")
            dataframe.to_sql(name=table_name, con=engine, schema=schema, 
                             if_exists=if_exists, chunksize=chunksize,
                             method=method, index=False)
            dataframe_not_inserted = pd.DataFrame()
            break

        except Exception as error:
            log = f":" if counter == 0 else "mensage was omitted."
            print(f"ATTEMPT NUMBER {counter}, error" + log)
            if counter == 1:
                print(error)
            dataframe_not_inserted = dataframe
        
        counter += 1
    return dataframe_not_inserted
        
        
@filter_warnings
def truncate_cascade_table(table_name, schema="flight"):
    """Truncate table using cascade method.
    
    Parameters
    ----------
    table_name: str
        The name of the database table.
    
    schema: str (default="flight")
        The name of the database schema.
    """
    conn = load_conn()
    curr = conn.cursor()
    curr.execute(f"TRUNCATE TABLE {schema}.{table_name} CASCADE")
    conn.commit()
    curr.close()
    conn.close()
