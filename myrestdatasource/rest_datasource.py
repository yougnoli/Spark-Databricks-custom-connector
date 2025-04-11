import requests
import json
import datetime
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from typing import Iterator

def flatten_json(nested, parent_key="", sep="."):
    """
    Basic recursive flatten of a JSON object (dict) into a one-level dict.
    E.g. {"a": {"b": 123, "c": 456}} -> {"a.b": 123, "a.c": 456}
    Arrays (lists) remain as raw JSON strings.
    """
    items = []
    if isinstance(nested, dict):
        for k, v in nested.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
    elif isinstance(nested, list):
        items.append((parent_key, json.dumps(nested)))
    else:
        items.append((parent_key, nested))
    return dict(items)

def get_nested_value(data, json_path):
    """
    Extracts a nested value from data following a path like "data.items".
    If any level is missing, returns None.
    """
    if not json_path:
        return data
    keys = json_path.split(".")
    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
        if data is None:
            return None
    return data

def infer_spark_type(value):
    """
    Infer Spark DataType from a given Python value.
    For strings, we try to detect an ISO-formatted datetime.
    For lists or dicts, we fallback to StringType since these are flattened to JSON strings.
    """
    if value is None:
        return StringType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()
    if isinstance(value, float):
        return DoubleType()
    if isinstance(value, str):
        try:
            # Attempt to parse ISO formatted datetime
            datetime.datetime.fromisoformat(value)
            return TimestampType()
        except ValueError:
            return StringType()
    return StringType()

def convert_value_to_type(value, spark_type):
    """
    Convert a value to the corresponding Python type matching the Spark DataType.
    """
    if value is None:
        return None

    if isinstance(spark_type, LongType):
        try:
            return int(value)
        except Exception:
            return None

    if isinstance(spark_type, DoubleType):
        try:
            return float(value)
        except Exception:
            return None

    if isinstance(spark_type, BooleanType):
        try:
            if isinstance(value, bool):
                return value
            value_lower = str(value).lower()
            return value_lower in ["true", "1", "yes", "t"]
        except Exception:
            return None

    if isinstance(spark_type, TimestampType):
        try:
            if isinstance(value, str):
                return datetime.datetime.fromisoformat(value)
            elif isinstance(value, datetime.datetime):
                return value
            else:
                return None
        except Exception:
            return None

    # Fallback to string conversion
    return str(value)

class MyRestDataSource(DataSource):
    """
    Spark Data Source V2 in Python to read any REST API.
    This data source attempts to infer schema dynamically.
    It supports the following options:
        .option("auth_token", "Bearer XYZ")
        .option("pagination", "true")
        .option("page_param", "page")
        .option("start_page", "1")
        .option("max_pages", "10")
        .option("json_path", "data.items")
        .option("base_url", "...")
        .option("endpoint", "...")
        .option("infer_types", "true")  # Optional: if set to true, infer types from the first record
    """

    @classmethod
    def name(cls):
        # Name used in spark.read.format("myrestdatasource")
        return "myrestdatasource"

    def schema(self):
        """
        Spark calls this method to get a schema (StructType)
        for the DataFrame.
        
        We perform a quick API call to infer the columns by examining the first JSON object.
        Each field is flattened and its type is inferred (if enabled) or set as a string.
        """
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        pagination = self.options.get("pagination", "false").lower() == "true"
        page_param = self.options.get("page_param", "page")
        start_page = int(self.options.get("start_page", 1))
        infer_types_flag = self.options.get("infer_types", "false").lower() == "true"

        params = {}
        if pagination:
            params[page_param] = start_page

        # Use a requests.Session for improved performance and connection reuse
        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            # Set a timeout to avoid hanging indefinitely
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

        # Apply json_path if present
        json_path = self.options.get("json_path")
        data = get_nested_value(data, json_path)
        if data is None:
            # No data returns an empty schema
            return StructType([])

        # If the root is a single object, wrap it in a list
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or len(data) == 0:
            return StructType([])

        # Infer columns based on the first element
        first_elem = data[0]
        if not isinstance(first_elem, dict):
            return StructType([])

        flattened = flatten_json(first_elem)
        fields = []
        for key, value in flattened.items():
            if infer_types_flag:
                spark_type = infer_spark_type(value)
            else:
                spark_type = StringType()
            fields.append(StructField(key, spark_type, True))

        return StructType(fields)

    def reader(self, schema):
        """
        Creates and returns a DataSourceReader that uses the schema
        determined in the schema() method.
        """
        return MyRestDataSourceReader(schema, self.options)


class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def read(self, partition) -> Iterator[tuple]:
        """
        Spark calls this on each partition (in this case, only one partition).
        We loop to handle pagination, retrieving and flattening each JSON object
        based on the inferred schema and converting each value to its proper type.
        """
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        pagination = self.options.get("pagination", "false").lower() == "true"
        page_param = self.options.get("page_param", "page")
        start_page = int(self.options.get("start_page", 1))
        max_pages = int(self.options.get("max_pages", 10))
        json_path = self.options.get("json_path")

        # Retrieve column names and their corresponding Spark types from the schema
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        page = start_page

        # Use a requests.Session for connection reuse and improved performance
        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            
            while True:
                params = {}
                if pagination:
                    params[page_param] = page
                
                resp = session.get(url, headers={}, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                data = get_nested_value(data, json_path)
                if data is None:
                    break

                # Normalize to a list if data is a dict
                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list) or len(data) == 0:
                    break

                for elem in data:
                    # Flatten the JSON element
                    flattened = flatten_json(elem)
                    row = []
                    # Build the row based on the schema and convert each value to the proper type
                    for col, spark_type in col_details:
                        val = flattened.get(col)
                        converted_val = convert_value_to_type(val, spark_type)
                        row.append(converted_val)
                    yield tuple(row)

                # Exit the loop if pagination is not enabled
                if not pagination:
                    break

                if page >= max_pages:
                    break
                page += 1

    def partitions(self):
        # Return a single partition since this example handles one partition only.
        return [InputPartition(0)]
