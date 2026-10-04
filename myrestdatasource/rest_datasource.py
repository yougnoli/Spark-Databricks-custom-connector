import requests
import json
import datetime
from dataclasses import dataclass
from typing import Iterator, Optional
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

def _fetch_page(url, params, auth_token=None, timeout=10):
    """
    Shared helper to fetch a single page of JSON data from the REST API.
    Used both by MyRestDataSource.schema() (to sample the first page while
    inferring the schema) and by MyRestDataSourceReader.read() (to fetch the
    page assigned to each partition), so the request logic isn't duplicated.
    """
    with requests.Session() as session:
        if auth_token:
            session.headers.update({"Authorization": auth_token})
        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

class MyRestDataSource(DataSource):
    """
    Spark Data Source V2 in Python to read any REST API.
    Requires Spark 4.0+ (Public Preview in Databricks Runtime 15.2 and above),
    since it relies on pyspark.sql.datasource.DataSource / DataSourceReader /
    InputPartition.

    This data source attempts to infer schema dynamically from the first
    sampled record, unless an explicit schema is provided via the "schema"
    option. It supports the following options:
        .option("auth_token", "Bearer XYZ")     # avoid hardcoding secrets, see the article
        .option("pagination", "true")
        .option("page_param", "page")
        .option("start_page", "1")
        .option("max_pages", "10")
        .option("json_path", "data.items")
        .option("base_url", "...")
        .option("endpoint", "...")
        .option("infer_types", "true")  # Optional: if set to true, infer types from the first record
        .option("schema", "<json.dumps(StructType.jsonValue())>")  # Optional: bypass inference entirely
    """

    @classmethod
    def name(cls):
        # Name used in spark.read.format("myrestdatasource")
        return "myrestdatasource"

    def schema(self):
        """
        Spark calls this method to get a schema (StructType)
        for the DataFrame.

        If the "schema" option is set, it is parsed directly and no API call
        is made here at all. Otherwise, we perform a quick API call to infer
        the columns by examining the first JSON object. Each field is
        flattened and its type is inferred (if enabled) or set as a string.
        """
        schema_option = self.options.get("schema")
        if schema_option:
            # Bypass automatic inference entirely using a user-supplied
            # schema. This is the recommended workaround for sparse APIs,
            # where the first record sampled for inference might have a
            # misleading null in a field that is numeric elsewhere.
            return StructType.fromJson(json.loads(schema_option))

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

        raw = _fetch_page(url, params, auth_token)

        # Apply json_path if present
        json_path = self.options.get("json_path")
        data = get_nested_value(raw, json_path)

        # Cache the first page so the reader doesn't have to fetch it again
        # for the partition responsible for `start_page`.
        self._cached_first_page = {"page": start_page if pagination else None, "data": data}

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
        determined in the schema() method, forwarding along the first page
        we may have already fetched while inferring that schema.
        """
        cached_first_page = getattr(self, "_cached_first_page", None)
        return MyRestDataSourceReader(schema, self.options, cached_first_page=cached_first_page)


@dataclass
class RestInputPartition(InputPartition):
    """
    One partition per page when pagination is enabled, so Spark can fetch
    pages in parallel across executors instead of looping through all of
    them sequentially inside a single task. `page` is None when pagination
    is disabled, since there is only one request to make.
    """
    page: Optional[int] = None


class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema, options, cached_first_page=None):
        self.schema = schema
        self.options = options
        self._cached_first_page = cached_first_page or {}

    def partitions(self):
        """
        Returns one partition per page when pagination is enabled (from
        start_page to max_pages), so each page is fetched by a separate
        Spark task. Returns a single partition when pagination is disabled,
        since there's only one request to make.
        """
        pagination = self.options.get("pagination", "false").lower() == "true"
        if not pagination:
            return [RestInputPartition(page=None)]

        start_page = int(self.options.get("start_page", 1))
        max_pages = int(self.options.get("max_pages", 10))
        return [RestInputPartition(page=p) for p in range(start_page, max_pages + 1)]

    def read(self, partition: RestInputPartition) -> Iterator[tuple]:
        """
        Spark calls this once per partition, i.e. once per page when
        pagination is enabled. We fetch that single page, flatten each JSON
        object and convert every value to the type declared in the schema.
        """
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        pagination = self.options.get("pagination", "false").lower() == "true"
        page_param = self.options.get("page_param", "page")
        max_pages = int(self.options.get("max_pages", 10))
        json_path = self.options.get("json_path")

        # Retrieve column names and their corresponding Spark types from the schema
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        page = partition.page

        # Reuse the first page already fetched by schema() on the driver,
        # when this partition happens to be the one responsible for it,
        # instead of hitting the API again for the exact same page.
        if self._cached_first_page and self._cached_first_page.get("page") == page:
            data = self._cached_first_page.get("data")
        else:
            params = {}
            if pagination:
                params[page_param] = page
            raw = _fetch_page(url, params, auth_token)
            data = get_nested_value(raw, json_path)

        if data is None:
            return

        # Normalize to a list if data is a dict
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or len(data) == 0:
            return

        if pagination and page == max_pages and len(data) > 0:
            # We stopped at max_pages but this page still returned data, so
            # there may be more pages beyond max_pages that were never
            # fetched. This fails silently unless we say something here.
            print(
                f"Warning: reached max_pages={max_pages} for endpoint '{endpoint}' "
                "while this page still returned data. Some records may be missing; "
                "increase the 'max_pages' option if you need the remaining pages."
            )

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
