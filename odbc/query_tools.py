import pandas as pd

from connection import load_conn


def get_table(table, condition=""):    
    query = f"""
            SELECT *
            FROM flight.{table}
        """
    with load_conn() as conn:
            table = pd.read_sql(query, conn)
    return table


def get_max_database_search_id(table="search"):
        query = f"""
            SELECT COALESCE(max(search_id), -1) AS max_search_id
            FROM flight.{table}
        """
        with load_conn() as conn:
            max_search_id = pd.read_sql(query, conn)
        max_search_id = max_search_id.loc[0, "max_search_id"]
        return max_search_id