import csv
import json
import os.path
import datetime
import os
import tempfile
import zipfile
from datetime import datetime

PIPELINE_SIZE = 100
DATA_DIR = os.path.abspath('./tests/data/')

def convert_to_unix_timestamp(dt):
    return int(dt.timestamp() * 1000)

def parse_timestamp_to_unix_millis(timestamp_str, fmt='%Y-%m-%d %H:%M:%S'):
    ts = datetime.strptime(timestamp_str, fmt)
    return convert_to_unix_timestamp(ts)

def load_power_consumption_data():
    # column indexes
    state_idx = 0
    region_idx = 1
    timestamp_column_idx = 2
    consumption_column_idx = 3

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = os.path.join(DATA_DIR, 'power_consumption_india_2019_2020.csv.zip')
        # Open the ZIP file
        with zipfile.ZipFile(data_path, 'r') as zip_ref:
            # Extract all contents to the temporary directory
            zip_ref.extractall(temp_dir)

        # Find the CSV file in the temporary directory
        csv_file = next(file for file in os.listdir(temp_dir) if file.endswith('.csv'))
        csv_path = os.path.join(temp_dir, csv_file)

        # Parse the timestamp and convert it to Unix timestamp
        # Read and print the CSV contents
        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                state = row[state_idx]
                # skip header row
                if state == 'States':
                    continue

                region = row[region_idx]
                timestamp = row[timestamp_column_idx]
                timestamp = parse_timestamp_to_unix_millis(timestamp, '%d/%m/%Y %H:%M:%S')
                consumption = float(row[consumption_column_idx])

                # print(f"region: {region}, state: {state}, ts: {timestamp}, consumption: {consumption}, ")
                # Create a TemperatureRecord object
                record = PowerConsumptionRecord(timestamp, state, region, consumption)

                yield record

            os.remove(csv_path)

class PowerConsumptionRecord:
    def __init__(self, timestamp, state, region, consumption):
        self.timestamp = timestamp
        self.region = region
        self.state = state
        self.consumption = float(consumption)

    def format_state(self):
        # Format the state name to match the expected format in the metric
        return self.state.lower().replace(' ', '_')
    def __repr__(self):
        return (f"PowerConsumptionRecord(timestamp={self.timestamp}, state={self.state}, region={self.region}, "
                f"consumption={self.consumption})")
    def key(self):
        state = self.format_state()
        return 'power_consumption:{}::{}'.format(state, self.region)
    def metric(self):
        state = self.format_state()
        return 'power_consumption{{state={},region="{}"}}'.format(state, self.region)

def load_json_rows(file_path):
    """
    Generator function to load rows from a JSON file.

    Args:
    file_path (str): Path to the JSON file.

    Yields:
    dict: Each row from the JSON file as a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            # Load the entire JSON content
            data = json.load(file)

            # Check if the loaded data is a list
            if isinstance(data, list):
                for row in data:
                    yield row
            # If it's a dictionary, yield it as a single item
            elif isinstance(data, dict):
                yield data
            else:
                raise ValueError("JSON file must contain a list of objects or a single object")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def load_text_rows(file_path):
    """
    Generator function to load rows from a text file.

    Args:
    file_path (str): Path to the text file.

    Yields:
    str: Each row from the text file.
    """
    try:
        with open(file_path, 'r') as file:
            for row in file:
                yield row
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def load_samples_from_file(filename, start=None, step=None):
    """
    Load a series from a file.

    Args:
    filename (str): Path to the file.

    Returns:
    list: A list of series data.
    """
    if start is None:
        start = datetime.now() - datetime.timedelta(days=1)  # Load data for the previous day

    # Calculate the interval between each data point
    if step is None:
        step = datetime.timedelta(minutes=5)

    timestamp = start
    for row in load_text_rows(filename):
        yield [timestamp, row]
        timestamp += step

def load_cpu_data(start=None, step=None):
    data_path = os.path.join(DATA_DIR, 'cpu-values.txt')
    return load_samples_from_file(data_path, start, step)

def load_memory_data(start=None, step=None):
    data_path = os.path.join(DATA_DIR, 'memory-values.txt')
    return load_samples_from_file(data_path, start, step)


def ingest_power_consumption_data(valkey_conn):
    print("Ingesting power consumption data ...")
    r = valkey_conn.pipeline(transaction=False)
    count = 0
    total_added = 0
    added_keys = set([])

    for data in load_power_consumption_data():
        key = data.key()

        if key not in added_keys:
            added_keys.add(key)
            state = data.state
            region = data.region
            # metric = data.metric()
            # print(f"Created series: {key}, metric={metric}")
            valkey_conn.execute_command('TS.CREATE', key,
                                        'ENCODING', 'COMPRESSED',
                                        'CHUNK_SIZE', '2ki',
                                        'DECIMAL_DIGITS', 2,
                                        'DUPLICATE_POLICY', 'LAST',
                                        'LABELS', '__name__', 'power_consumption', 'state', state, 'region', region)

        r.execute_command('TS.ADD', key, data.timestamp, data.consumption)
        count += 1
        total_added += 1
        if count == PIPELINE_SIZE:
            # print(f"Executing pipeline with {count} records...")
            r.execute()
            count = 0
            r = valkey_conn.pipeline(transaction=False)

    r.execute()
    print(f"Added {total_added} records to Valkey.")