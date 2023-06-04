import pandas as pd

import database_tools as dt
import query_tools as qt


# Alter this line
# date = "2023-06-03"

# The list of tables has to be in that order because
# it is not possible to delete the search lines if a
# searchId exists in another table
for table_name in ["flight", "fare", "search"]:
    print(f"date: {date} table_name: {table_name}", "-"*50)
    query = f"""
    SELECT *
    FROM flight.{table_name}
    WHERE DATE_TRUNC('day', "insertionTime") = DATE '{date}'
    """

    table = qt.run_query(query)
    print("Len table: ", len(table))

    dt.delete_rows_by_insertionTime(date, table_name)

    table = run_query(query)
    print("Len table: ", len(table))
