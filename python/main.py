from pyspark.sql import SparkSession

# Create or get existing SparkSession (Databricks-managed)
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Sample data
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "John"),
    (4, "Basha"),
    (5, "lal"),
    (6, "johnny")
]

# Create DataFrame with schema
df = spark.createDataFrame(data, ["id", "name"])

# Display DataFrame
df.show()

# Print schema
df.printSchema()
