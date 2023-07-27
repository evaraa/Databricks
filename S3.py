from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Suresh") \
    .getOrCreate()

# Set the AWS access key and secret key
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAVDKIIDQNB3I5GAUG")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# Specify the S3 path for the CSV file
# s3_path = "s3a://evaraa/rootkey.csv"
s3_path = "s3a://evaraa/customers-1000000.csv"

# Read the CSV file into a DataFrame
df1 = spark.read.csv(s3_path, header=True)

# Show the DataFrame
df1.show()