import pandas as pd

from connection import load_conn
from filter_warnings import filter_warnings


@filter_warnings
def get_table(table, condition="", schema="flight"):
    """Get any table on database.
    
    Parameters
    ----------
    table: str
        Table name.
    
    condition: str (default="")
        Any SQL condition. Eg: where serchID = 1
    
    Return
    ------
    table: pd.DataFrame
        Table returned by query.
    """
    query = f"""
            SELECT *
            FROM {schema}.{table}
            {condition}
        """
    with load_conn() as conn:
        table = pd.read_sql(query, conn)
    return table


@filter_warnings
def get_max_search_id(table="search", schema="flight"):
    """Get max search_id.
    
    Parameters
    ----------
    table: str
        Table name.
    
    Return
    ------
    max_search_id: int
        Maximum existing search_id in the table. 
    """
    query = f"""
        SELECT COALESCE(max("searchId"), -1) AS max_search_id
        FROM {schema}.{table}
    """
    with load_conn() as conn:
        max_search_id = pd.read_sql(query, conn)
    max_search_id = max_search_id.loc[0, "max_search_id"]
    return max_search_id


@filter_warnings
def run_query(query):
    """Run any query on database.
    
    Parameters
    ----------
    query: str
        SQL query.
    
    Return
    ------
    dataframe: pd.DataFrame
        DataFrame returned by query.
    """
    with load_conn() as conn:
        dataframe = pd.read_sql(query, conn)
    return dataframe


def list_database_processes():
    """Retrieve information about processes in the database.

    The function filters out processes where 'usename' is not NULL, ensuring that only valid
        processes associated with a specific username are included in the result.

    Return
    ------
    database_processes: pd.DataFrame
        A pd.DataFrame containing the following columns:
            - pid: Process ID.
            - duration: Time duration since the query started executing.
            - usename: Username of the process.
            - datname: Name of the current database.
            - query: Query being executed by the process.
            - state: Current state of the process.
            - wait_event_type: Type of event the process is waiting for (if applicable).
            - wait_event: Name of the event the process is waiting for (if applicable).
            - client_addr: IP address of the client connected to the process.        
    """
    query = """
    SELECT pid, now() - query_start as duration,
        usename, datname, query, state,
        wait_event_type, wait_event,
        client_addr
    FROM pg_stat_activity
    WHERE usename IS NOT NULL
    ORDER BY duration DESC
    """

    database_processes = run_query(query)
    return database_processes
