# spark-snowflake-nycTaxiProject

This project demonstrates a simple batch data pipeline using Apache Spark to process NYC taxi trip data, followed by exporting the results to Snowflake using the pandas connector.

## 📁 Project Components

- `batch_pipeline.py`: Main Spark job to load, clean, and aggregate hourly fare stats.
- `load_to_snowflake.py`: Loads the resulting summary into a Snowflake table.
- `config.py`: Holds your Snowflake credentials and connection parameters.
- `requirements.txt`: Dependencies for local execution.
- `.gitignore`: Ignore compiled or sensitive files.

## ⚙️ Setup

### 1. Installing dependencies

```bash
pip install -r requirements.txt
```

### 2. Preparing files

- Place the raw file `yellow_tripdata_sample.csv` in the same directory.
- Make sure `config.py` is properly filled with your Snowflake credentials.

### 3. Running batch pipeline

```bash
python batch_pipeline.py
```

This will generate:
- `trip_summary.csv`: A file containing average fare and trip count by pickup hour.

### 4. Loading to Snowflake

```bash
python load_to_snowflake.py
```

This will:
- Create the table `TRIP_SUMMARY` if it doesn’t exist.
- Upload the `trip_summary.csv` to your Snowflake database.

## 🧪 Sample Output

```
+------------+----------+-----------+
|pickup_hour | avg_fare | trip_count|
+------------+----------+-----------+
|     0      |   10.23  |   145     |
|     1      |   9.78   |   120     |
...
```
