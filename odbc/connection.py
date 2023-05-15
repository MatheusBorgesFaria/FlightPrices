import json

import psycopg2


def load_conn(config_file="../settings/database_config.json", config_dict=None):
    """Creates psycopg2 connection to a PostgreSQL database.

    Parameters
    ----------
        config_file (str): The path to a JSON file containing the configuration
            parameters. Defaults to "../settings/database_config.json".
        config_dict (dict): A dictionary containing the configuration parameters.
            If this argument is None, the function will use this dictionary
            instead of reading from the JSON file.

    Return
    ------
        A psycopg2 database connection object.
    """
    if config_dict is not None:
        config = config_dict
    else:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )
    
    return conn
