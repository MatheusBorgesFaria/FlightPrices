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
                             max_n_attempts=5, 
                             temporarily_disable_table_indexes=True):
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
    temporarily_disable_table_indexes: bool (defaul=True)
        Se True drop table index and recreate it after isertion data.

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
    
    if temporarily_disable_table_indexes:
        drop_index(table_name, schema=schema)
    
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
    
    if temporarily_disable_table_indexes:
        create_table_index(table_name, schema=schema)
        reindex(index_name=f"{table_name}_pkey", schema=schema)
        
    
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
    return


def reindex(table_name=None, index_name=None, schema="flight"):
    """Reindex the table.
    
    Parameters
    ----------
    table_name: str (default=None)
        The name of the database table.
    index_name: str (default=None)
        The name of the index to be reindexed.
    schema: str (default="flight")
        The name of the database schema.
    """
    assert table_name is not None or index_name is not None, (
        "You have to pass the table_name or index_name parameter"
    )
    conn = load_conn()
    cursor = conn.cursor()
    try :
        if isinstance(table_name, str):
            command = f"REINDEX TABLE {schema}.{table_name}"
            print(command)
            cursor.execute(command)
        elif isinstance(index_name, str):
            command = f"REINDEX INDEX {schema}.{index_name}"
            print(command)
            cursor.execute(command)
        print("Done!")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(str(e))
    finally:
        cursor.close()
        conn.close()
    return


def kill_database_processes(database_processes):
    """Kill all processes in the database.

    Parameters
    ----------
    database_processes : pd.DataFrame
        DataFrame containing information about the processes in the database.
    """
    conn = load_conn()
    cursor = conn.cursor()

    try:
        for pid in database_processes['pid'].unique():
            cursor.execute(f"SELECT pg_terminate_backend({pid})")
        conn.commit()
        print("All processes killed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error killing processes: {str(e)}")
    finally:
        cursor.close()
        conn.close()
    return

    
def drop_index(table_name, schema="flight"):
    """Drop indexes on a specific table in a PostgreSQL database. 

    Parameters
    ----------
    table_name: str
        The name of the table.
    schema: str (default="flight")
        The name of the schema where the table is located.
    """
    conn = load_conn()
    cursor = conn.cursor()

    try:
        query_get_table_indexs = f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = '{schema}' AND tablename = '{table_name}'
        """
        indexes = qt.run_query(query_get_table_indexs)
        for index_name, index_def in zip(indexes["indexname"], indexes["indexdef"]):
            if "UNIQUE" in index_def:
                continue
            command = f"""DROP INDEX {schema}."{index_name}"; """
            cursor.execute(command)
            print(command)
        conn.commit()
        print(f"Table indexes {schema}.{table_name} successfully droped.")
    except Exception as e:
        conn.rollback()
        print(f"Error indexes for table {schema}.{table_name}: {str(e)}")
    finally:
        cursor.close()
        conn.close()
    return


def delete_rows_by_insertionTime(date, table_name, schema="flight"):
    """Delete rows from a table based on a specific insertionTime date value.

    Parameters
    ----------
    date: str
        The date value to match (format: 'YYYY-MM-DD').
    table_name: str
        The name of the table.
    schema: str (default="flight")
        The name of the schema where the table is located.
    """
    query = f"""
    DELETE FROM {schema}.{table_name}
    WHERE DATE_TRUNC('day', "insertionTime") = DATE '{date}'
    """
    conn = load_conn()
    cursor = conn.cursor()
    try :
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(str(e))
    finally:
        cursor.close()
        conn.close()
    return


def create_table_index(table_name, schema="flight"):
    """Create indexes for a specified table.

    This function creates indexes for the specified table based on a pre-defined configuration.

    Parameters
    ----------
    table_name: str
        The name of the table for which indexes will be created.
    schema: str (default="flight")
        The name of the schema where the table is located.   
    """
    indexes_config = {
        "search": [
            {"unique":"UNIQUE", "index_name":"search_pkey", "column":"searchId"},
            {"unique":"", "index_name":"operationalSearchTime_index", "column":"operationalSearchTime"},
            {"unique":"", "index_name":"origin_destination_code_index", "column":"""originCode", "destinationCode"""}
        ],
        "flight": [
            {"unique":"UNIQUE", "index_name":"flight_pkey", "column":"searchId"},
            {"unique":"", "index_name":"legId_flight_index", "column":"legId"}
        ],
        "fare": [
            {"unique":"UNIQUE", "index_name":"fare_pkey", "column":"searchId"},
            {"unique":"", "index_name":"legId_fare_index", "column":"legId"},
            {"unique":"", "index_name":"totalFare_index", "column":"totalFare"}
        ]
    }
    assert table_name in indexes_config.keys(), (
        f"We only accept {indexes_config.keys()} tables. You asked for the {table_name} table"
    )
    indexes_to_create = indexes_config.get(table_name, [])

    conn = load_conn()
    cursor = conn.cursor()

    try:
        for index_to_create in indexes_to_create:
            unique = index_to_create.get("unique", "")
            index_name = index_to_create.get("index_name")
            column = index_to_create.get("column")
            command = f"""
                CREATE {unique} INDEX IF NOT EXISTS "{index_name}"
                ON {schema}.{table_name} USING btree ("{column}")
            """
            print(command, "...")
            cursor.execute(command)
        conn.commit()
        print("All indexes created successfully!")

    except Exception as e:
        conn.rollback()
        print(str(e))

    finally:
        cursor.close()
        conn.close()
    return
