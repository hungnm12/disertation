from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import os

app = Flask(__name__)

# Initialize the Spark session (can be moved inside the function for on-demand initialization)
def get_spark_session():
    return SparkSession.builder \
        .appName("S3 to Cloud SQL") \
        .config("spark.jars", "/path/to/mysql-connector-java.jar") \
        .getOrCreate()

@app.route('/load_data_to_cloud_sql', methods=['POST'])
def load_data_to_cloud_sql():
    try:
        # Get parameters from the request (e.g., S3 path, Cloud SQL details)
        data = request.json
        s3_path = data.get("s3_path")
        cloud_sql_properties = data.get("cloud_sql_properties")

        # Initialize Spark session
        spark = get_spark_session()

        # Set S3 credentials (if needed)
        spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
        spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
        spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

        # Load data from S3 into a Spark DataFrame
        df = spark.read.option("header", "true").csv(s3_path)

        # Write the DataFrame to Cloud SQL (MySQL)
        df.write.jdbc(url=cloud_sql_properties["url"], table=cloud_sql_properties["table"], mode="overwrite", properties=cloud_sql_properties)

        spark.stop()

        return jsonify({"status": "success", "message": "Data loaded to Cloud SQL successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/load_data_to_mongo', methods=['POST'])
def load_data_to_mongo():
    try:
        # Get parameters from the request (e.g., S3 path, MongoDB URI)
        data = request.json
        s3_path = data.get("s3_path")
        mongo_uri = data.get("mongo_uri")
        mongo_db = data.get("mongo_db")
        mongo_collection = data.get("mongo_collection")

        # Initialize Spark session with MongoDB Connector
        spark = SparkSession.builder \
            .appName("S3 to MongoDB") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .getOrCreate()

        # Load data from S3 into a Spark DataFrame
        df = spark.read.option("header", "true").csv(s3_path)

        # Write the DataFrame to MongoDB Atlas
        df.write.format("mongo").mode("append").option("database", mongo_db).option("collection", mongo_collection).save()

        spark.stop()

        return jsonify({"status": "success", "message": "Data loaded to MongoDB Atlas successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    # Make sure to run the server on your desired host and port
    app.run(host='0.0.0.0', port=5000)
