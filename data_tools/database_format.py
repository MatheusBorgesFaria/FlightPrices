import sys

import numpy as np
import pandas as pd
import reverse_geocoder as rg  # ADICIONAR NO REQUIREMENTS
from joblib import Parallel, delayed

sys.path.append("../odbc")
import query_tools as qt
from connection import load_conn

from pdb import set_trace

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
        self.conn = load_conn()
        self.unique_value_tables = ["airport", "airline", "equipment"]
        self.tables_columns = {
            "search": ["search_id", "search_time", "operational_search_time", "flight_day",
                       "origin_code", "destination_code"],
            "flight": ["search_id", "legId", "travelDuration", "duration", "durationInSeconds",
                       "elapsedDays", "isNonStop", "departureTimeRaw",
                       "departureTimeZoneOffsetSeconds", "arrivalTimeRaw",
                       "arrivalTimeZoneOffsetSeconds", "flightNumber", "stops", "airlineCode",
                       "equipmentCode", "arrivalAirportCode", "departureAirportCode"],
            "fare": ["search_id", "legId", "fareBasisCode", "isBasicEconomy", "isRefundable",
                     "isFreeChangeAvailable", "taxes", "fees", "showFees",
                     "currency", "baseFare", "totalFare", "numberOfTickets",
                     "freeCancellationBy", "hasSeatMap", "providerCode", "seatsRemaining"],
            "airport": ["airportCode", "city", "AirportLatitude", "AirportLongitude"],
            "airline": ["airlineCode", "airlineName", "externalAirlineCode", "operatingAirlineName"],
            "equipment": ["equipmentCode", "equipmentDescription"],
        }
    
    def __del__(self):
        self.conn.close()
    
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
        tables_list = Parallel(n_jobs=n_jobs, prefer="threads", verbose=1)(
            [delayed(self._transform_parquet)(parquet_path)
             for parquet_path in self.parquet_paths]
        )
        
        tables = self._post_processing(tables_list)
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
        data.reset_index(names="search_id", inplace=True)
        tables = {}
        tables["search"] = self._transform_search_data(data)
        tables["flight"] = self._transform_flight_data(data)
        tables["fare"] = self._transform_fare_data(data)
        tables["airport"] = self._transform_airport_data(data)
        tables["airline"] = self._transform_airline_data(data)
        tables["equipment"] = self._transform_equipment_data(data)
        return tables
    
    def _transform_search_data(self, data):
        """Transform structured raw data into search table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        search_data: pd.DataFrame
            Data in search table format.
        """
        search_data = data[self.tables_columns["search"]]
        return search_data
    
    def _transform_flight_data(self, data):
        """Transform structured raw data into flight table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        flight_data: pd.DataFrame
            Data in flight table format.
        """
        flight_data = data[self.tables_columns["flight"]]
        return flight_data
    
    def _transform_fare_data(self, data):
        """Transform structured raw data into fare table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        fare_data: pd.DataFrame
            Data in fare table format.
        """
        fare_data = data[self.tables_columns["fare"]]
        return fare_data
    
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
        airport_data = airport_data.drop_duplicates()

        columns = self.tables_columns["airport"].copy()
        columns.remove('city')
        return airport_data[columns]
    
    def _transform_airline_data(self, data):
        """Transform structured raw data into airline table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        airline_data: pd.DataFrame
            Data in airline table format.
        """
        columns = self.tables_columns["airline"].copy()
        airline_data = data[columns].drop_duplicates()
        return airline_data
    
    def _transform_equipment_data(self, data):
        """Transform structured raw data into equipment table format.

        Parameters
        ----------
        data: pd.DataFrame
            Structured raw data

        Return
        ------
        equipment_data: pd.DataFrame
            Data in equipment table format.
        """
        columns = self.tables_columns["equipment"].copy()
        equipment_data = data[columns].drop_duplicates()
        return equipment_data
    
    def _post_processing(self, tables_list):
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
        
        # Post-processing of tables 
        next_search_id = qt.get_max_database_search_id() + 1
        for table_key, table in tables.items():
            tables[table_key] = pd.concat(table, ignore_index=True)
            
            # Fix search_id column
            if "search_id" in self.tables_columns[table_key]:
                end_search_id = next_search_id + len(tables[table_key])
                tables[table_key]["search_id"] = range(next_search_id, end_search_id)
            
            # Get unique values
            if table_key in self.unique_value_tables:
                database_table = qt.get_table(table_key)
                tables[table_key] = pd.concat([tables[table_key], database_table])
                tables[table_key] = self._get_unique_values(
                    tables[table_key], tables[table_key].columns
                )
                
                # Add city column on airport table
                if "airport" in table_key:
                    # TEM ERRO AQUI
                    set_trace()
                    coordinates = list(
                        zip(tables[table_key]["AirportLatitude"],
                            tables[table_key]["AirportLongitude"])
                    )
                    airport_data["city"] = self.get_city_from_coordinates(coordinates)                    
            
        return tables

    def _get_unique_values(self, dataframe, columns):
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
        dataframe = dataframe.drop_duplicates()

        concatenated_df = None
        for column in dataframe.columns:
            df = dataframe[column].str.split(pat=self.separator, regex=False, expand=True)
            df = df.melt(var_name=f"{column}_variable", value_name = column)

            if concatenated_df is None:
                concatenated_df = df
            else:
                concatenated_df = pd.concat([concatenated_df, df], axis=1)

        unique_values = (
            concatenated_df
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
        results = rg.search(coordinates)
        citys_list = [city.get("name") for city in results]
        return citys_list
    