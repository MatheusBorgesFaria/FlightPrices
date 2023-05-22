import sys

import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from joblib import Parallel, delayed

sys.path.append("../odbc")
import database_tools as dt
import query_tools as qt
from connection import load_conn


class DatabaseFormat:
    """Transform structured raw data into database format.
    
    The database has 6 tables in schema flight:
    - search
    - flight
    - fare
    - airport
    - airline
    - equipment
    """

    def __init__(self, parquet_paths, separator="||"):
        """
        Parameters
        ----------
        parquet_paths: list[str]
            List of parquet's path whose data should be structured into database format.
        separator: str (default="||")
            Some flights have more than one leg, that is, there are stops between
            the initial and final destination. Therefore, these flights have
            information for each segment separated by separator="||".
        """
        self.parquet_paths = parquet_paths
        self.separator = separator
        self.unique_value_tables = ["airport", "airline", "equipment"]
        self.tables_columns = {
            "search": ["searchId", "searchTime", "operationalSearchTime", "flightDay",
                       "originCode", "destinationCode"],
            "flight": ["searchId", "legId", "travelDuration", "duration", "durationInSeconds",
                       "elapsedDays", "isNonStop", "departureTimeRaw",
                       "departureTimeZoneOffsetSeconds", "arrivalTimeRaw",
                       "arrivalTimeZoneOffsetSeconds", "flightNumber", "stops", "airlineCode",
                       "equipmentCode", "arrivalAirportCode", "departureAirportCode",
                       "arrivalAirportLatitude", "arrivalAirportLongitude",
                       "departureAirportLatitude", "departureAirportLongitude"],
            "fare": ["searchId", "legId", "fareBasisCode", "isBasicEconomy", "isRefundable",
                     "isFreeChangeAvailable", "taxes", "fees", "showFees",
                     "currency", "baseFare", "totalFare", "numberOfTickets",
                     "freeCancellationBy", "hasSeatMap", "providerCode", "seatsRemaining"],
            "airport": ["airportCode", "city", "airportLatitude", "airportLongitude"],
            "airline": ["airlineCode", "airlineName", "externalAirlineCode", "operatingAirlineName"],
            "equipment": ["equipmentCode", "equipmentDescription"],
        }

    def transform_all_parquets(self, n_jobs=-1, inset_on_database=False):
        """Transform structured raw data from all parquet into database format

        Parameters
        ----------
        n_jobs: int (default=-1, all cores)
            Number of colors.
        
        push_in_database: bool (default=False)
            If True push data in database.
        Return
        ------
        tables: dict[pd.DataFrame]
            Dictionary with all tables in the database format.
        """
        tables_list = Parallel(n_jobs=n_jobs, prefer="processes", verbose=1)(
            [delayed(self._transform_parquet)(parquet_path)
             for parquet_path in self.parquet_paths]
        )
        
        tables = self._post_processing(tables_list)
        
        if inset_on_database:
            print("Saving data...")
            for table_name, table in tables.items():
                print(f"Saving {table_name} table... {len(table)} lines")
                if_exists = "replace" if table_name in self.unique_value_tables else "append"
                dt.insert_database(table, table_name, if_exists=if_exists)
        
        return tables

    def _transform_parquet(self, parquet_path):
        """Transform structured raw data from 1 parquet into database format.

        Parameters
        ----------
        parquet_path: str
            One parquet path.

        Return
        ------
        tables: dict[pd.DataFrame]
            Dictionary with all tables in the database format.
        """
        data = pd.read_parquet(parquet_path)
        data.reset_index(drop=True, inplace=True)
        data.reset_index(names="searchId", inplace=True)

        rename_search_table = {
            "search_time": "searchTime",
             "operational_search_time": "operationalSearchTime",
             "flight_day":  "flightDay",
             "origin_code": "originCode",
             "destination_code": "destinationCode"
        }
        data.rename(columns=rename_search_table, inplace=True)
        tables = {}
        for table_name, table_columns in self.tables_columns.items():
            if "airport" in table_name:
                    tables[table_name] = self._transform_airport_data(data)
            else:
                tables[table_name] = data[table_columns]
            
            if table_name in self.unique_value_tables:                
                tables[table_name] = tables[table_name].drop_duplicates()
        return tables

    def _transform_airport_data(self, data):
        """Transform structured raw data into airport table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        airport_data: pd.DataFrame
            Data in airport table format.
        """
        origin_column = ["departureAirportCode","origin_city",
                         "departureAirportLatitude", "departureAirportLongitude"]
        destination_column = ["arrivalAirportCode", "destination_city",
                              "arrivalAirportLatitude", "arrivalAirportLongitude"]

        origin_rename = {origin_column[index] : self.tables_columns["airport"][index]
                         for index in range(len(origin_column))}
        destination_rename = {destination_column[index] : self.tables_columns["airport"][index]
                              for index in range(len(destination_column))}

        data_origin = data[origin_column].rename(columns=origin_rename)
        data_destination = data[destination_column].rename(columns=destination_rename)

        airport_data = pd.concat([data_origin, data_destination])

        columns = self.tables_columns["airport"].copy()
        columns.remove('city')
        return airport_data[columns]

    def _post_processing(self, tables_list):
        """Post-process the data from the parquets. 

        Parameters
        ----------
        tables_list: list[dict[pd.DataFrame]]
            A list with multiple _transform_parquet outputs.

        Return
        ------
        tables: dict[pd.DataFrame]
             Dictionary with all tables in the database format.
        """
        print("Post-processing the data...")
        print("Create only one table dict")
        # Create only one table dict
        create_table_key = True
        tables = {}
        for partial_tables in tables_list:
            for table_key, table in partial_tables.items():
                if create_table_key:
                    tables[table_key] = [table]
                else:
                    tables[table_key].append(table)
            create_table_key = False
        
        print("Post-processing of tables: fix search_id column; get unique values;")
        # Post-processing of tables 
        next_search_id = qt.get_max_search_id() + 1
        for table_key, table in tables.items():
            tables[table_key] = pd.concat(table, ignore_index=True)
            
            # Fix search_id column
            if "searchId" in self.tables_columns[table_key]:
                end_search_id = next_search_id + len(tables[table_key])
                tables[table_key]["searchId"] = range(next_search_id, end_search_id)
            
            # Get unique values
            if table_key in self.unique_value_tables:
                database_table = qt.get_table(table_key)
                tables[table_key] = pd.concat([tables[table_key], database_table])
                tables[table_key] = self._get_unique_values(tables[table_key])
        
        print("Adding city column on airport table")
        # Add city column on airport table
        coordinates = list(
            zip(tables["airport"]["airportLatitude"],
                tables["airport"]["airportLongitude"])
        )
        tables["airport"]["city"] = self.get_city_from_coordinates(coordinates)                    
            
        return tables

    def _get_unique_values(self, dataframe, columns=None):
        """Gets the unique values of a detaframe considering flight leg.

        Parameters
        ----------
        dataframe: pd.DataFrame
            Dataframe to get the unique.

        columns: list
            Columns to get unique values.

        Return
        ------
        unique_values: pd.DataFrame
            Unique values.
        """
        if columns is None:
            columns = dataframe.columns[~dataframe.isna().all()]
        dataframe = dataframe.drop_duplicates()

        concatenated_df = None
        for column in columns:
            df = dataframe[column].str.split(pat=self.separator, regex=False, expand=True)
            df = df.melt(var_name=f"{column}_variable", value_name = column)

            if concatenated_df is None:
                concatenated_df = df
            else:
                concatenated_df = pd.concat([concatenated_df, df], axis=1)

        unique_values = (
            concatenated_df[columns]
            .drop_duplicates()
            .replace("", np.nan)
            .dropna(how="any")
            .reset_index(drop=True)
        )
        return unique_values

    def get_city_from_coordinates(self, coordinates):
        """Define citys based on coordinates.

        Parameters
        ----------
        coordinates: list[tuple[float]]
            The coordinates of each city.
        
        Return
        ------
        citys_list: list
            A list of city names.
        """
        geolocator = Nominatim(user_agent="my-app")
        def get_city_name(coordinate):
            location = geolocator.reverse(coordinate, exactly_one=True)
            address = location.raw['address']
            city = address.get('city')
            return city
        citys_list = [get_city_name(coordinate) for coordinate in coordinates]
        return citys_list