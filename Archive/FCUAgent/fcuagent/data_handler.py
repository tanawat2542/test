import logging
import pandas as pd
from crate import client


def _execute_query_string(cratedb_config: dict(), query_string: str):
    """
    Query data from specified datasource with specified query string.

    Args:
        query_string (str): SQL Query string to be executed
        datasource (str): Datasource to query data from. Default is 'cratedb'

    Returns:
        data (list): List of data from CrateDB. Each element is a dictionary with column name as keys.

        data = [{'timestamp': 1675245600000,
               'location': 'chiller_plant/iot_devices',
               'device_id': 'CSQ_plant',
               'subdevice_idx': 0,
               'type': 'calculated_power',
               'aggregation_type': 'avg_1h',
               'datapoint': 'power',
               'value': '1289.8812590049934'}, ....]

    """
    cursor = None
    res = list()
    try:
        cratedb_url = str(cratedb_config.get('host', None)) + ':' + str(cratedb_config.get('port', None))
        connection = client.connect(cratedb_url, username=cratedb_config.get('username', None), password=cratedb_config.get('password', None))
        cursor = connection.cursor()
        cursor.execute(query_string)
        datas = cursor.fetchall()
        connection.close()

        column_names = [desc[0] for desc in cursor.description]

        # Preprocess list-of-lists into list-of-dicts with correct keys and values
        for row in datas:
            res.append({k: v for k, v in zip(column_names, row)})

        return res

    except Exception as e:
        logging.debug(f"Data could not be queried: {e} host: {cratedb_url} username: {cratedb_config.get('username', None)} password: {cratedb_config.get('password', None)}")
    finally:
        if cursor:
            cursor.close()


def _convert_timestamp_column_to_datetime_index(df: pd.DataFrame, timestamp_column: str = 'timestamp', timestamp_unit: str = 'ms'):
    """
    Preprocess timeseries data with timestamp column

    Args:
        df (pd.DataFrame): Dataframe to be preprocessed
        timestamp_column (str): Name of the timestamp column
        timestamp_unit (str): Unit of the timestamp column (ms, s, us, ns, etc.)

    Returns:
        df (pd.DataFrame): Preprocessed dataframe

    """
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit=timestamp_unit, utc=True)  # 2022-08-28 17:37:03+00:00
    df[timestamp_column] = df[timestamp_column].dt.tz_convert("Asia/Bangkok")  # 2022-08-28 00:37:03+07:00
    df[timestamp_column] = df[timestamp_column].dt.tz_localize(None)  # 2022-08-28 17:37:03
    df = df.rename(columns={timestamp_column: 'datetime'})

    df = df.set_index('datetime')

    return df


def _pre_process_timeseries_data(data: list, **kwargs):
    """
    Post-process the raw data into a dataframe with datetime as index

    Args:
        data (list): List of data from database. Each element is a dictionary with column name as keys.
        kwargs (dict): Dictionary of arguments to specify how to post-process the data. Avaiable arguments are as below
        - pivot_datapoint_column (bool): If True, pivot datapoint column to be columns of the dataframe

    """
    df = pd.DataFrame(data)

    if 'timestamp' in df.columns:
        df = _convert_timestamp_column_to_datetime_index(df, timestamp_column='timestamp', timestamp_unit='ms')
    # Pivot the datapoint column if specified
    if kwargs.get('pivot_datapoint_column', True):
        df = df.pivot_table(index='datetime', columns=['device_id', 'datapoint'], values='value', aggfunc='first')
        df = df.stack('device_id').reset_index('device_id')
        for col in df.columns:
            try:
                df[col] = df[col].astype(float)
            except:
                pass
                # logging.debug(f"Column [{col}] cannot be converted to float")

    return df


def query_data_from_database(cratedb_config: dict(), filters: dict, **kwargs):
    """
    Query data from datasource in config

    Args:
        filters (dict): Dictionary of filters to apply to the query with the format below
        filters = {                             |   ex.     filters = {
            <column_name_1>: {                  |               timestamp: {
                <operator_1>: <value_1>,        |                   ">": 100000,
                <operator_2>: <value_2>,        |                   "<": 200000
                ...                             |               },
            },                                  |               device_id: {
            <column_name_2>: ...                |                   "=": "device_1"
        }                                       |               }
                                                |           }
    Supported operators: "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE"

        **kwargs: Additional arguments to be passed to the query function
        - table_name (str): Name of the table to query from CrateDB
        - location (str): Location of the data to query from CosmosDB
        - pivot_datapoint_column (bool): Whether to pivot the datapoint column or not

    Returns:
        df (pd.DataFrame): Dataframe of queried and preprocessed data

    """

    # Step 1: Parse kwargs to get relevant variables
    table_name = kwargs.get('table_name', 'raw_data')
    query_string = "SELECT * FROM {} WHERE ".format(table_name)
    prefix = ""
    col_prefix = ""

    # Step 2: Generate query string from the given filters dictionary
    for col_name, f in filters.items():

        for oper, value in f.items():

            if value is None:
                logging.debug(f"Invalid value for column [{col_name}] -- value = {value}")
                continue

            # Preprocess value to be compatible with CrateDB query string
            if isinstance(value, str):
                value_string = f"'{value}'"
            elif isinstance(value, list):
                value_string = str(tuple(value)) if len(value) > 1 else str(tuple(value)).replace(",", "")
            else:
                value_string = str(value)

            # Add filter to query string
            if oper in [">", "<", ">=", "<=", "=", "!=", "LIKE", "NOT LIKE"]:
                query_string += f"{prefix} {col_prefix+col_name} {oper} {value_string}"
                prefix = "\nAND"
            elif oper.upper() in ["IN", "NOT IN"] and isinstance(value, list):
                query_string += f"{prefix} {col_prefix+col_name} {oper.upper()} {value_string}"
                prefix = "\nAND"
            else:
                print(f"Invalid filter specified for querying data from CrateDB -- {col_name}: {f}")

    # Step 3: Query raw data from specific datasource
    logging.debug(f"Querying data from Database: {query_string}")
    data: list = _execute_query_string(cratedb_config, query_string)
    logging.debug(f"Finished querying data from Database")
    if not data:
        logging.debug(f"No data found for query in Database: {query_string}")
        return pd.DataFrame()

    # Step 4: Post-processing the raw data according to each datasource
    df = _pre_process_timeseries_data(data, **kwargs)

    return df