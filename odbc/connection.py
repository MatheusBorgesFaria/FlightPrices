import json

import psycopg2
from sqlalchemy import create_engine


def load_conn(config_file="../settings/database_config.json", config_dict=None,
              connection_type="psycopg2"):
    """Creates psycopg2 or engine connection to a PostgreSQL database.

    Parameters
    ----------
        config_file: str (default="../settings/database_config.json")
            The path to a JSON file containing the configuration
            parameters.

        config_dict: dict (default=None)
            A dictionary containing the configuration parameters.
            If this argument is None, the function will use this dictionary
            instead of reading from the JSON file.

        connection_type: str (default="psycopg2")
            Connection type, valid values are ["engine", "psycopg2"].
    Return
    ------
        A psycopg2 or engine database connection object.
    """
    assert connection_type in ["engine", "psycopg2"], "connection_type must be equal 'engine' or 'psycopg2' "
    
    if config_dict is not None:
        config = config_dict
    else:
        with open(config_file, 'r') as f:
            config = json.load(f)
    
    if connection_type == "psycopg2":
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
    elif connection_type == "engine":
        conn_str = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        engine = create_engine(conn_str)
        conn = engine
    return conn
