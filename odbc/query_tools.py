import pandas as pd

from connection import load_conn


def get_table(table, condition=""):
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
            FROM flight.{table}
            {condition}
        """
    with load_conn() as conn:
            table = pd.read_sql(query, conn)
    return table


def get_max_search_id(table="search"):
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
            FROM flight.{table}
        """
        with load_conn() as conn:
            max_search_id = pd.read_sql(query, conn)
        max_search_id = max_search_id.loc[0, "max_search_id"]
        return max_search_id