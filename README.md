# Spark REST API Connector (Python Data Source V2)

This repository contains a custom Spark Data Source connector implemented in Python, designed to load data from REST APIs directly into Spark DataFrames using the Data Source V2 API. It supports:

- Dynamic schema inference from nested JSON, with an optional explicit `schema` override for sparse APIs
- Pagination across API responses, with one Spark partition fetched per page for real parallelism
- Robust type inference and conversion
- Efficient HTTP session management via a shared fetch helper, reused between schema inference and reading
- Easy deployment to Azure Databricks (non-community edition)

📄 **Full narrative write-up on Medium:**
[Creating Your Own Spark Databricks Connector for REST APIs: Mastering Data Ingestion with the Spark Data Source API](https://medium.com/@tugnolialessio/creating-your-own-spark-databricks-connector-for-rest-apis-mastering-data-ingestion-with-the-spark-06653f2d18d9)

This README is the technical reference: all the code and its explanation live here and in `myrestdatasource/rest_datasource.py`.

---

## ✅ Requirements

- **Spark 4.0+** (Public Preview in Databricks Runtime 15.2 and above) — the connector is built on `pyspark.sql.datasource.DataSource`, `DataSourceReader` and `InputPartition`, which don't exist on older runtimes. On an unsupported runtime, `from myrestdatasource import MyRestDataSource` will fail with an `ImportError`.
- **`requests` installed on every cluster node**, not just the driver/notebook environment. `schema()` runs on the driver, but `read()` runs on executors — if `requests` is only `pip install`-ed in the notebook, executors will fail with `ModuleNotFoundError` the first time they fetch a page. Install it as a cluster library instead (it's already declared in `setup.py`'s `install_requires`, but that only covers the environment where the wheel itself is installed).

---

## 📁 Folder Structure

```
.
├── setup.py
├── myrestdatasource/
│   ├── __init__.py
│   ├── rest_datasource.py
├── Spark-Databricks-Connector-REST-API-Test.ipynb
```

- `setup.py`: Script for building the package.
- `myrestdatasource/rest_datasource.py`: The full implementation of the connector.
- `Spark-Databricks-Connector-REST-API-Test.ipynb`: Hands-on tests against real public APIs (JSONPlaceholder, ReqRes, Random User API, a Postman mock server).

---

## 🚀 Quick Start

```python
from myrestdatasource import MyRestDataSource
spark.dataSource.register(MyRestDataSource)  # only needs to run once per Spark session

df = (spark.read
      .format("myrestdatasource")
      .option("infer_types", "true")
      .option("pagination", "false")
      .option("base_url", "https://jsonplaceholder.typicode.com")
      .option("endpoint", "posts")
      .load())

df.show()
```

### How to Build and Install the Wheel

1. From the repo root, run:

```bash
python setup.py bdist_wheel
```

2. The wheel is generated inside `dist/`.
3. In Azure Databricks, go to **Compute > Libraries > Install New > Upload**, upload the `.whl`, and attach it to your cluster.

---

## 🧩 How It Works

The connector has two moving parts: `MyRestDataSource`, which Spark calls to get a schema, and `MyRestDataSourceReader`, which Spark calls to actually fetch rows.

### Schema Inference (`MyRestDataSource.schema()`)

By default, the schema is inferred from the **first record** returned by the endpoint (after applying `json_path`, if set). Each key is flattened (nested objects become dotted names like `location.street.name`; arrays are kept as JSON strings) and its Spark type is guessed from the value (`infer_spark_type`): booleans, integers (`LongType`), floats (`DoubleType`), ISO-formatted date strings (`TimestampType`), and everything else as `StringType`.

Because inference looks at a single record, a sparse API where that first record has a `null` in a field that's numeric elsewhere will get that column permanently typed as a string, and later numeric values will be silently coerced to strings. To avoid this, pass the `schema` option with the JSON representation of a `StructType` (e.g. `df.schema.json()` from an existing DataFrame with the shape you want) — when this option is set, `schema()` parses it directly via `StructType.fromJson(...)` and skips the API call entirely:

```python
schema_json = df.schema.json()

df2 = (spark.read
      .format("myrestdatasource")
      .option("base_url", "https://jsonplaceholder.typicode.com")
      .option("endpoint", "posts")
      .option("schema", schema_json)
      .load())
```

While inferring the schema, `schema()` also **caches the first page it fetched** (`self._cached_first_page`) and hands it to the reader via `reader(schema)`, so that page isn't requested a second time during `read()`.

### Parallel Pagination (`MyRestDataSourceReader`)

`partitions()` returns **one `RestInputPartition` per page** — from `start_page` to `max_pages` (default `10`) — when `pagination` is enabled, so Spark fetches pages in parallel across executors instead of looping through them sequentially in a single task. With pagination disabled, it returns a single partition for the one request that's needed.

`read(partition)` fetches exactly the page that partition owns (reusing the cached first page when it matches), flattens each JSON element with `flatten_json`, and converts every value to the schema's declared type with `convert_value_to_type`.

If the **last** page fetched (the one at `max_pages`) still returns data, `read()` prints a warning, since that's a sign there may be more pages beyond what you asked for:

```
Warning: reached max_pages=10 for endpoint 'users' while this page still
returned data. Some records may be missing; increase the 'max_pages' option
if you need the remaining pages.
```

### Shared HTTP Fetching

Both `schema()` and `read()` go through a single helper, `_fetch_page(url, params, auth_token, timeout)`, built on `requests.Session`. This avoids duplicating request/timeout/auth-header logic in two places, and keeps the retry/timeout behavior consistent between schema inference and actual reads.

### A Note on Secrets

`auth_token`, like every other `.option(...)` value, is a plain string that can surface in the Spark UI or driver logs. Don't hardcode a real token as shown in the Quick Start comments below — pull it from `dbutils.secrets.get(scope, key)` or an environment variable instead:

```python
df = (spark.read
      .format("myrestdatasource")
      .option("auth_token", f"Bearer {dbutils.secrets.get('my-scope', 'api-token')}")
      .option("base_url", "https://api.example.com")
      .option("endpoint", "orders")
      .load())
```

If you're obtaining that token from Keycloak via a Client Credentials flow, see [API Authentication and Authorization with Keycloak and Data API Builder in Docker](https://medium.com/@tugnolialessio/api-authentication-and-authorization-with-keycloak-and-data-api-builder-in-docker-91ad6cf20a45) and [Implementing a Secure On-Premises API with Data API Builder, Keycloak, and SQL Server](https://medium.com/@tugnolialessio/implementing-a-secure-on-premises-api-with-data-api-builder-keycloak-and-sql-server-8d9fbed2871e).

---

## ⚙️ All Options

- `base_url`, `endpoint`: required. Combined (with a trailing slash stripped) into the request URL.
- `auth_token`: optional. Sent as the `Authorization` header, e.g. `"Bearer xyz"`.
- `pagination`: `"true"`/`"false"` (default `"false"`).
- `page_param`: query parameter name used for the page number (default `"page"`).
- `start_page`: first page number to fetch (default `1`).
- `max_pages`: last page number to fetch (default `10`). A warning is printed if the last page fetched still returns data.
- `json_path`: dotted path to the array/object to extract from the response, e.g. `"data.items"`.
- `infer_types`: `"true"`/`"false"` (default `"false"`). When `"false"`, every column is `StringType`.
- `schema`: optional JSON representation of a `StructType` (e.g. `df.schema.json()`) to bypass inference entirely.

---

## 📦 Repository Goals

This connector was built from scratch to solve repetitive tasks when ingesting data from REST APIs into Spark. It removes boilerplate code and provides a clean, production-ready interface.

**Current limitations** (see the Medium article's "Future Improvements" section): there's no automatic retry or backoff on HTTP failures or rate limits, and since pagination now fetches pages in parallel across partitions, a rate limit is more likely to be hit than with a slower sequential loop.

---
