# Spark REST API Connector (Python Data Source V2)

This repository contains a custom Spark Data Source connector implemented in Python, designed to load data from REST APIs directly into Spark DataFrames using the Data Source V2 API. It supports:

- Dynamic schema inference from nested JSON, with an optional explicit `schema` override for sparse APIs
- Pagination across API responses, with one Spark partition fetched per page for real parallelism
- Robust type inference and conversion
- Optional custom HTTP headers (via the `headers` option) for APIs that authenticate through something other than an `Authorization` header, e.g. `x-api-key`
- Efficient HTTP session management via a shared fetch helper, reused between schema inference and reading
- Tested on **Databricks Free Edition** (serverless compute); classic clusters (e.g. Azure Databricks) are also supported

📄 **Full narrative write-up on Medium:**
[Build Your Own Spark/Databricks Connector for REST APIs](https://medium.com/@tugnolialessio/build-your-own-spark-databricks-connector-for-rest-apis-06653f2d18d9)

📄 **And on my website:**
[Build Your Own Spark/Databricks Connector for REST APIs](https://www.tugnolialessio.com/blog/spark-databricks-connector-rest-apis/)

This README is the technical reference: all the code and its explanation live here and in `myrestdatasource/rest_datasource.py`.

---

## ✅ Requirements

- **Spark 4.0+** (generally available on Databricks Runtime 15.4 LTS and above, per Databricks' own GA announcement — this covers classic clusters too) — the connector is built on `pyspark.sql.datasource.DataSource`, `DataSourceReader` and `InputPartition`, which don't exist on older runtimes. On an unsupported runtime, `from myrestdatasource import MyRestDataSource` will fail with an `ImportError`. I tested this on Databricks Free Edition, whose serverless compute, at the time of writing, reports `spark.version` as `4.2.0`.
- **`requests` installed on every node that runs Spark tasks**, not just the driver/notebook environment. `schema()` runs on the driver, the reads run on executors — on serverless too. `%pip install` (or a cluster library on a classic cluster) reaches both; `!pip install` only reaches the driver, and executors then fail with `ModuleNotFoundError` the first time they fetch a page. This package ships `requests` via `install_requires`, so installing the wheel covers it either way.
- **`wheel` installed in whatever local Python environment you use to build the package** (`pip install wheel`). `python setup.py bdist_wheel` doesn't work out of the box — `bdist_wheel` is a command contributed by the `wheel` package itself, not by `setuptools`. Skip this and you'll hit `error: invalid command 'bdist_wheel'` instead of a `.whl` file in `dist/`. This step happens on your laptop/CI, not on Databricks, so it has nothing to do with whether you're on serverless or a classic cluster.

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
- `Spark-Databricks-Connector-REST-API-Test.ipynb`: Hands-on tests against real public APIs (JSONPlaceholder, ReqRes, Random User API, and httpbin.org's `/bearer` endpoint for token-protected auth — all of them public, with nothing to configure before running the notebook).

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

If `myrestdatasource` isn't importable yet, you haven't built and installed the wheel — jump to "Installing on Databricks Free Edition" (or the classic-cluster section right after it) below first.

### Installing on Databricks Free Edition (tested, primary path)

Free Edition runs on serverless compute only, so there's no classic "Compute > Libraries" screen to upload a wheel to a cluster. Here's what I actually did:

1. Make sure `wheel` is installed locally, then build the wheel from the repo root:

```bash
pip install wheel
python setup.py bdist_wheel
```

This produces `dist/myrestdatasource-0.3.0-py3-none-any.whl`.

2. In your Free Edition workspace, open **Catalog**, pick (or create) a Unity Catalog Volume — e.g. `/Volumes/workspace/default/libraries/` — and upload the `.whl` file there through the Catalog Explorer's upload button.
3. In a notebook cell, install it from the volume path with `%pip install` (not `!pip install` — see the requirements note above on why that distinction matters):

```python
%pip install /Volumes/workspace/default/libraries/myrestdatasource-0.3.0-py3-none-any.whl
```

4. Restart the Python process so the new package is picked up, then import and register as usual:

```python
dbutils.library.restartPython()
```

```python
from myrestdatasource import MyRestDataSource
spark.dataSource.register(MyRestDataSource)
```

### Installing on Azure Databricks / classic clusters (secondary path)

If you're on a classic (non-serverless) cluster instead:

1. Make sure `wheel` is installed locally, then from the repo root run:

```bash
pip install wheel
python setup.py bdist_wheel
```

2. The wheel is generated inside `dist/`.
3. Go to **Compute > Libraries > Install New > Upload**, upload the `.whl`, and attach it to your cluster.

---

## 🧩 How It Works

The connector has two moving parts: `MyRestDataSource`, which Spark calls to get a schema, and `MyRestDataSourceReader`, which Spark calls to actually fetch rows.

### Schema Inference (`MyRestDataSource.schema()`)

By default, the schema is inferred from the **first record** returned by the endpoint (after applying `json_path`, if set). Each key is flattened (nested objects become dotted names like `location.street.name`; arrays are kept as JSON strings) and its Spark type is guessed from the value (`infer_spark_type`): booleans, integers (`LongType`), floats (`DoubleType`), ISO-formatted date strings (`TimestampType`), and everything else as `StringType`.

Because inference looks at a single record, a sparse API where that first record has a `null` in a field that's numeric elsewhere will get that column permanently typed as a string, and later numeric values will be silently coerced to strings. To avoid this, pass the `schema` option with the JSON representation of a `StructType` (e.g. `df.schema.json()` from an existing DataFrame with the shape you want) — when this option is set, `schema()` parses it directly via `StructType.fromJson(...)` and skips the API call entirely:

```python
schema_json = df.schema.json()  # df was read from the same endpoint below

df2 = (spark.read
      .format("myrestdatasource")
      .option("base_url", "https://jsonplaceholder.typicode.com")
      .option("endpoint", "posts")
      .option("schema", schema_json)
      .load())
```

The field names in that schema have to match the flattened field names of the endpoint you're applying it to — reusing a schema from a *different* endpoint will silently leave every unmatched column `null` rather than raising an error.

While inferring the schema, `schema()` also **caches the first page it fetched** (`self._cached_first_page`) and hands it to the reader via `reader(schema)`, so that page isn't requested a second time during `read()`.

### Parallel Pagination (`MyRestDataSourceReader`)

`partitions()` returns **one `RestInputPartition` per page** — from `start_page` to `max_pages` (default `10`) — when `pagination` is enabled, so Spark fetches pages in parallel across executors instead of looping through them sequentially in a single task. With pagination disabled, it returns a single partition for the one request that's needed.

There's no early-stop logic: a partition is created for every page number in the `start_page`–`max_pages` range regardless of whether the API actually has that many pages. Set `max_pages` close to the real page count — too low triggers the warning below, too high just means extra, mostly-empty requests up to that number.

`read(partition)` fetches exactly the page that partition owns (reusing the cached first page when it matches), flattens each JSON element with `flatten_json`, and converts every value to the schema's declared type with `convert_value_to_type`.

If the **last** page fetched (the one at `max_pages`) still returns data, `read()` prints a warning, since that's a sign there may be more pages beyond what you asked for:

```text
Warning: reached max_pages=10 for endpoint 'users' while this page still
returned data. Some records may be missing; increase the 'max_pages' option
if you need the remaining pages.
```

### Shared HTTP Fetching

Both `schema()` and `read()` go through a single helper, `_fetch_page(url, params, auth_token, headers, timeout)`, built on `requests.Session`. This avoids duplicating request/timeout/auth-header logic in two places, and keeps the timeout behaviour consistent between schema inference and actual reads.

`headers` (parsed by `_get_headers_option`, which raises a clear `ValueError` if the option isn't valid JSON, isn't a JSON object, or contains a non-string value — without ever echoing the actual option value or header values back in the error message, since this is exactly where an API key would be) is applied to the session first, then `auth_token` is applied on top as the `Authorization` header. So if `headers` happens to also define `Authorization`, `auth_token` wins — it's the more explicit, single-purpose option of the two.

### A Note on Secrets

`auth_token` and `headers`, like every other `.option(...)` value, are plain strings that can surface in the Spark UI or driver logs. Don't hardcode a real token or key as a literal string — pull it from `dbutils.secrets.get(scope, key)` or an environment variable instead:

```python
df = (spark.read
      .format("myrestdatasource")
      .option("auth_token", f"Bearer {dbutils.secrets.get('my-scope', 'api-token')}")
      .option("base_url", "https://api.example.com")
      .option("endpoint", "orders")
      .load())
```

For an API that authenticates through a custom header instead of `Authorization` (`x-api-key`, for instance), use `headers` the same way — secret value in, JSON object out:

```python
import json

df = (spark.read
      .format("myrestdatasource")
      .option("headers", json.dumps({"x-api-key": dbutils.secrets.get("my-scope", "api-key")}))
      .option("base_url", "https://api.example.com")
      .option("endpoint", "orders")
      .load())
```

### A Note on Error Messages over Spark Connect

The test notebook includes a case where the `/bearer` call is made without a token, to confirm the connector lets the resulting `401` propagate instead of swallowing it (there's no retry/backoff logic, see "Repository Goals" below). On a classic cluster, catching that error and printing it gives you a reasonably short `PYTHON_DATA_SOURCE_ERROR` with a `requests.exceptions.HTTPError` inside it.

On **Databricks Free Edition's serverless compute**, which runs through **Spark Connect**, the same `except Exception as e` still catches the error, but `str(e)` can be much longer: Spark Connect embeds the full server-side execution stack (frames like `ExecuteThreadRunner`, `UCSEphemeralState`, `DBRTracing` — Databricks' own plumbing, not this connector) underneath the actual Python exception. That's expected behavior for Spark Connect, not a sign that something is broken. The notebook works around the noise by printing only the exception type and the first line of the message, which is where the real `HTTPError` text shows up. Exact exception type and message formatting can vary across Databricks Runtime / Spark Connect versions, so treat that as a starting point rather than a guaranteed match.

---

## ⚙️ All Options

- `base_url`, `endpoint`: required. Combined (with a trailing slash stripped) into the request URL.
- `auth_token`: optional. Sent as the `Authorization` header, e.g. `"Bearer xyz"`.
- `headers`: optional. JSON object (as a string) of extra HTTP headers merged into the same session, e.g. `'{"x-api-key": "xyz"}'` — for APIs that authenticate through a header other than `Authorization`. Raises a `ValueError` if the value isn't valid JSON, isn't an object, or contains a non-string value (HTTP header values must be strings); the error message never echoes the option's value, to avoid leaking a key into logs over a JSON typo. If `headers` also sets `Authorization` and `auth_token` is set too, `auth_token` wins.
- `pagination`: `"true"`/`"false"` (default `"false"`).
- `page_param`: query parameter name used for the page number (default `"page"`).
- `start_page`: first page number to fetch (default `1`).
- `max_pages`: last page number to fetch (default `10`). A partition is created for every page in range regardless of how many pages actually exist; a warning is printed if the last page fetched still returns data.
- `json_path`: dotted path to the array/object to extract from the response, e.g. `"data.items"` — this is where the actual list of records lives inside the response envelope, as opposed to metadata the API might wrap around it (e.g. `total_pages`).
- `infer_types`: `"true"`/`"false"` (default `"false"`). When `"false"`, every column is `StringType`.
- `schema`: optional JSON representation of a `StructType` (e.g. `df.schema.json()`) to bypass inference entirely. Field names must match the target endpoint's flattened fields.

---

## 📦 Repository Goals

This connector was built from scratch to solve repetitive tasks when ingesting data from REST APIs into Spark. It removes boilerplate code and provides a clean, production-ready interface.

**Current limitations** (see the article's "Tested Against Real APIs" section): there's no automatic retry or backoff on HTTP failures or rate limits, and since pagination now fetches pages in parallel across partitions, a rate limit is more likely to be hit than with a slower sequential loop. Error handling today covers malformed or missing JSON fields (nulls, unexpected nesting, missing keys) — not HTTP-level failures. The `headers` option covers static, pre-obtained credentials (an API key you already have); there's no support for token refresh, signing, or anything dynamic beyond what you pass in yourself.

---
