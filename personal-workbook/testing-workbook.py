# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read CSV with Schema") \
    .getOrCreate()

# Read the schema from the JSON file
with open('path/to/your_schema.json', 'r') as file:
    schema_json = json.load(file)
    input_schema = StructType.fromJson(schema_json)

# Read the CSV file using the schema
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(input_schema) \
    .load("path/to/your_file.csv")

# Show the DataFrame
df.show()

