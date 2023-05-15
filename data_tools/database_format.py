import pandas as pd
from joblib import Parallel, delayed

import sys

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

    def __init__(self, parquet_paths):
        self.parquet_paths = parquet_paths
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
            "airport": ["airportCode", "AirportLatitude", "AirportLongitude", "city"],
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
        return
    
    def _get_airline_data(self, data):
        return
    
    def _get_equipment_data(self, data):
        return