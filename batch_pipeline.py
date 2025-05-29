from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Batch Processing") \
    .getOrCreate()

# Load CSV with inferred schema
df = spark.read.csv("yellow_tripdata_sample.csv", header=True, inferSchema=True)

# Show inferred schema and sample data
print("✅ Schema Inferred:")
df.printSchema()
df.show(5)

# Use correct column names from inferred schema (adjust as needed)
pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else df.columns[0]
fare_col = "fare_amount" if "fare_amount" in df.columns else df.columns[-1]
trip_distance_col = "trip_distance" if "trip_distance" in df.columns else df.columns[-2]

# Clean and transform
df_clean = df.filter((col(fare_col) > 0) & (col(trip_distance_col) > 0))
df_transformed = df_clean.withColumn("pickup_hour", hour(col(pickup_col)))

# Aggregate by hour
summary_df = df_transformed.groupBy("pickup_hour").agg(
    avg(fare_col).alias("avg_fare"),
    count("*").alias("trip_count")
)

# Show result
summary_df.orderBy("pickup_hour").show()

# Convert to Pandas before stopping Spark
pdf = summary_df.toPandas()

# Save to CSV
pdf.to_csv("trip_summary.csv", index=False)

# Stop Spark
spark.stop()

print("✅ Data processing complete. CSV saved as trip_summary.csv.")
