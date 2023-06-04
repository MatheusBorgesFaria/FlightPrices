import json
import sys
from datetime import datetime
from os.path import join
from time import time

import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from joblib import Parallel, delayed

sys.path.append("../odbc")
import database_tools as dt
import query_tools as qt
from connection import load_conn

sys.path.append("../utils")
from tools import get_relevant_path


class DatabaseFormat:
    """Transform structured raw data into database format.
    
    The database has 6 tables in schema flight:
    - search
    - flight
    - fare
    - airport
    - airline
    - equipment
    - data_upload
    """

    def __init__(self, parquet_paths, separator="||", next_search_id=None,
                 inset_on_database=False, bypass_table_insert=None):
        """
        Parameters
        ----------
        parquet_paths: list[str]
            List of parquet's path whose data should be structured into database format.
        separator: str (default="||")
            Some flights have more than one leg, that is, there are stops between
            the initial and final destination. Therefore, these flights have
            information for each segment separated by separator="||".
        next_search_id: int (default=None)
            Next value of the search_id column. This is used in tables: search, flight, fare.
        inset_on_database: bool (default=False)
            If True insert data in database.
        bypass_table_insert: list (default=None)
            List of tables not to upload to the database
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
        if next_search_id is None:
            self.next_search_id = qt.get_max_search_id() + 1
        else:
            assert isinstance(next_search_id, int), (
                f"next_search_id must be int, it is {type(next_search_id)}"
            )
            self.next_search_id = next_search_id
        
        assert isinstance(inset_on_database, bool), (
                f"inset_on_database must be bool, it is {type(inset_on_database)}"
            )
        self.inset_on_database = inset_on_database
            
        if bypass_table_insert is None:
            bypass_table_insert = []
        else:
            assert isinstance(bypass_table_insert, list), (
                f"bypass_table_insert must be list, it is {type(bypass_table_insert)}"
            )
            self.bypass_table_insert = bypass_table_insert
        

    def transform_all_parquets(self, n_jobs=-1):
        """Transform structured raw data from all parquet into database format

        Parameters
        ----------
        n_jobs: int (default=-1, all cores)
            Number of colors.

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
        
        if self.inset_on_database:
            print("Saving data...")
            for table_name, table in tables.items():
                if table_name in self.bypass_table_insert:
                    print(f"Skipping {table_name}, it has {len(table_name)} lines.")
                    continue
                
                start_time = time()
                if_exists = "replace" if table_name in self.unique_value_tables else "append"
                print(f"Saving {table_name} table... {len(table)} lines, if_exists = {if_exists}")
                dataframe_not_inserted = dt.insert_database_parallel(table, table_name, if_exists=if_exists)
                end_time = time()
                print(f"Done in {(end_time - start_time)/60} min!")
                
                self.save_dataframe_not_inserted(dataframe_not_inserted, table_name)

            dataframe_not_inserted = self.insert_data_upload_table(self.parquet_paths)
            self.save_dataframe_not_inserted(dataframe_not_inserted, "data_upload")
        
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
        for table_key, table in tables.items():
            tables[table_key] = pd.concat(table, ignore_index=True)
            
            # Fix search_id column
            if "searchId" in self.tables_columns[table_key]:
                end_search_id = self.next_search_id + len(tables[table_key])
                tables[table_key]["searchId"] = range(self.next_search_id, end_search_id)
            
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

    @staticmethod
    def save_dataframe_not_inserted(dataframe_not_inserted, table_name):
        """Saves data that could not be inserted to database.
        
        The dataframe_not_inserted will be saved in the path returned by the 
        utils.tools.get_relevant_path("database_format_not_inserted") function.
        
        Parameters
        ----------
        dataframe_not_inserted: pd.DataFrame
            Datarframe that could not be inserted to database.
        
        table_name: str
            The table name
        """
        if not dataframe_not_inserted.empty:
            print(f"Saving dataframe_not_inserted, {len(dataframe_not_inserted)} "
                  f"lines, table = {table_name}")
             
            file_name = (table_name + "_" + datetime.now().strftime("%Y%m%d_%Hh_%mmin")
                         + ".parquet")
            save_path = join(get_relevant_path("database_format_not_inserted"), file_name)
            dataframe_not_inserted.to_parquet(save_path)
    
    @staticmethod
    def insert_data_upload_table(file_paths):
        """Prepare and insert the data into the data_upload table.
        
        Parameters
        ----------
        file_paths: list
            List of file paths
        
        Return
        ------
        dataframe_not_inserted: pd.DataFrame
            Datarframe that could not be inserted to database.
        """
        assert isinstance(file_paths, list), "file_paths must be list."
        data_upload = pd.DataFrame({"filePath": file_paths})
        dataframe_not_inserted = dt.insert_database_parallel(data_upload, "data_upload")
        return dataframe_not_inserted