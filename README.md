# Spark REST API Connector (Python Data Source V2)

This repository contains a custom Spark Data Source connector implemented in Python, designed to load data from REST APIs directly into Spark DataFrames using the Data Source V2 API. It supports:

- Dynamic schema inference from nested JSON  
- Pagination across API responses  
- Robust type inference and conversion  
- Efficient HTTP session management  
- Easy deployment to Azure Databricks (non-community edition)

---

## 📁 Folder Structure

```
.
├── setup.py
├── myrestdatasource/
│   ├── __init__.py
│   ├── myrestdatasource.py
.
```

- `setup.py`: Script for building the package.  
- `myrestdatasource/`: Contains the implementation of the connector.

---

## ⚙️ How to Build the Package

To create the `.whl` file needed for Databricks:

1. Navigate to the root of the project (where `setup.py` is located).
2. Run the following command:

```bash
python setup.py bdist_wheel
```

3. The wheel file will be generated inside the `dist/` directory.

---

## 🚀 How to Install on Azure Databricks

You can easily upload and use the connector in your Azure Databricks workspace:

1. Go to **Compute > Libraries** in your Databricks UI.  
2. Click **Install New > Upload**.  
3. Upload the `.whl` file generated in the previous step.  
4. Once installed, attach the library to your cluster.  
5. You can now use the connector in a notebook like this:

```python
from myrestdatasource import MyRestDataSource
spark.dataSource.register(MyRestDataSource)

df = (spark.read
      .format("myrestdatasource")
      #.option("auth_token", "Bearer xyz")
      .option("infer_types", "true")
      .option("pagination", "false")
      .option("page_param", "page")
      .option("start_page", "1")
      .option("max_pages", "1")
      .option("json_path", "")
      .option("base_url", "https://jsonplaceholder.typicode.com")
      .option("endpoint", "posts")
      .load())

df.show()
```

---

## 📦 Repository Goals

This connector was built from scratch to solve repetitive tasks when ingesting data from REST APIs into Spark. It removes boilerplate code and provides a clean, production-ready interface.

---
