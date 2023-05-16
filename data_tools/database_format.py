import sys

import numpy as np
import pandas as pd
import reverse_geocoder as rg  # ADICIONAR NO REQUIREMENTS
from joblib import Parallel, delayed

sys.path.append("../odbc")
from connection import load_conn


class DatabaseFormat:
    """Transform structured data into database format.
    
    The database has 6 tables in schema flight:
    - search
    - flight
    - fare
    - airport
    - airline
    - equipment
    """

    def __init__(self, parquet_paths, separator="||"):
        self.parquet_paths = parquet_paths
        self.separator = separator
        self.conn = load_conn()
        
        self.tables_columns = {
            "search": ["search_Id", "search_time", "operational_search_time", "flight_day",
                      "origin_code", "destination_code", "arrivalAirportCode"],
            "flight": ["search_Id", "legId", "travelDuration", "duration", "durationInSeconds",
                       "elapsedDays", "isNonStop", "departureTimeRaw",
                       "departureTimeZoneOffsetSeconds", "arrivalTimeRaw",
                       "arrivalTimeZoneOffsetSeconds", "flightNumber", "stops", "airlineCode",
                       "equipmentCode"],
            "fare": ["search_Id", "legId", "fareBasisCode", "isBasicEconomy", "isRefundable",
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
        return
        
    def _transform_parquet(self, parquet_path):
        data = pd.read_parquet(parquet_path)
        data.reset_index(drop=True, inplace=True)
        data.reset_index(names="search_Id", inplace=True)
        tables = {}
        tables["search"] = self._get_search_data(data)
        tables["flight"] = self._get_flight_data(data)
        tables["fare"] = self._get_fare_data(data)
        tables["airport"] = self._get_airport_data(data)
        tables["airline"] = self._get_airline_data(data)
        tables["equipment"] = self._get_equipment_data(data)
        return tables
    
    def _get_search_data(self, data):
        return
    
    def _get_flight_data(self, data):
        return
    
    def _get_fare_data(self, data):
        return
    
    def _get_airport_data(self, data):
        origin_column = ["departureAirportCode","origin_city",
                         "departureAirportLatitude", "departureAirportLongitude"]
        destination_column = ["arrivalAirportCode", "destination_city",
                              "arrivalAirportLatitude", "arrivalAirportLongitude"]

        origin_rename = {origin_column[index] : tables_columns["airport"][index]
                         for index in range(len(origin_column))}
        destination_rename = {destination_column[index] : tables_columns["airport"][index]
                              for index in range(len(destination_column))}

        data_origin = data[origin_column].rename(columns=origin_rename)
        data_destination = data[destination_column].rename(columns=destination_rename)

        dataframe = pd.concat([data_origin, data_destination])
        
        columns = tables_columns["airport"].copy()
        columns.remove('city')
        airport_data = get_unique_values(dataframe, columns)
        
        coordinates = list(
            zip(airport_data["AirportLatitude"], airport_data["AirportLongitude"])
        )
        citys = self.get_city_from_coordinates(coordinates)
        airport_data["city"] = citys
        return airport_data
    
    def _get_airline_data(self, data):
        columns = tables_columns["airline"].copy()
        airline_data = get_unique_values(data[columns], columns)
        return airline_data
    
    def _get_equipment_data(self, data):
        columns = tables_columns["equipment"].copy()
        equipment_data = get_unique_values(data[columns], columns)
        return equipment_data

    def get_unique_values(self, dataframe, columns):
        concatenated_df = None
        for column in columns:
            df = dataframe[column].str.split(pat=self.separator, regex=False, expand=True)
            df = df.melt(var_name=f"{column}_variable", value_name = column)

            if concatenated_df is None:
                concatenated_df = df
            else:
                concatenated_df = pd.concat([concatenated_df, df], axis=1)

        concatenated_df = (
            concatenated_df[columns]
            .drop_duplicates()
            .replace("", np.nan)
            .dropna(how="any")
            .reset_index(drop=True)
        )
        return concatenated_df

    def get_city_from_coordinates(self, coordinates):
        """Define citys based on coordinates

        Parameters
        ----------
        coordinates: list[tuple[float]]
        """
        results = rg.search(coordinates)
        citys_list = [city.get("name") for city in results]
        return citys_list