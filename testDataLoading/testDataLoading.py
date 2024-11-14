from pyspark.sql import SparkSession
from pyspark import SparkConf
import json

# Google Cloud credentials path (replace with your path)
GCS_CREDENTIALS_PATH = r"E:\vua\s3-key.json"
GCS_BUCKET = "gs://test-datalake-hung/listingsAndReviews.json"
MONGODB_ATLAS_URI = "mongodb+srv://hungnguyenmanh2k2:hung@testdb.7nrtx.mongodb.net/?retryWrites=true&w=majority"
MONGO_COLLECTION = "sample"

# Configure Spark with GCS and MongoDB properties
conf = SparkConf() \
    .setAppName("GCS to MongoDB Atlas") \
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .set("spark.mongodb.input.uri", MONGODB_ATLAS_URI) \
    .set("spark.mongodb.output.uri", MONGODB_ATLAS_URI) \
    .set("google.cloud.auth.service.account.enable", "true") \
    .set("google.cloud.auth.service.account.json.keyfile", GCS_CREDENTIALS_PATH) \
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_CREDENTIALS_PATH)

# Initialize Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Read data from GCS
gcs_data = spark.read.option("header", "true").csv(GCS_BUCKET)

# Print schema for verification
gcs_data.printSchema()

# Transform or process data as needed (e.g., filter, aggregate, etc.)

# Write data to MongoDB Atlas
gcs_data.write \
    .format("mongo") \
    .mode("append") \
    .option("database", "<database>") \
    .option("collection", MONGO_COLLECTION) \
    .save()

# Stop Spark session
spark.stop()
