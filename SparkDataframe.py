from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .getOrCreate()

# Path to the Parquet file in filestore
parquet_path = "/home/jmckinstry/Documents/GitHub/pythondataframe/fileStore/data.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_path)

# Show the DataFrame contents
df.show()

# Remove "high" from the first and fourth columns
df = df.withColumn(df.columns[0], regexp_replace(df[df.columns[0]], "high", "")) \
       .withColumn(df.columns[3], regexp_replace(df[df.columns[3]], "high", ""))

for c in df.columns:
    df = df.withColumn(c, regexp_replace(col(c), "big", "large"))

# Show the DataFrame contents
df.show()

# Stop the Spark session
spark.stop()