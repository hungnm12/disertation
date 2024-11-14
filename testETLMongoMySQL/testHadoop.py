from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import pymongo

app = Flask(__name__)

# Spark session
spark = SparkSession.builder \
    .appName("MongoDB to MySQL Migration") \
    .config("spark.jars", r"E:\mysql-connector-java\mysql-connector-j-9.1.0\mysql-connector-java-9.1.0.jar") \
    .getOrCreate()



# Flatten MongoDB documents
def flatten_document(document, parent_key=""):
    items = []
    if isinstance(document, dict):
        for key, value in document.items():
            new_key = parent_key + "." + key if parent_key else key
            flattened_value = flatten_document(value, new_key)  # Flatten nested docs
            if isinstance(flattened_value, tuple) and len(flattened_value) == 2:
                items.append(flattened_value)
            else:
                items.append((new_key, str(flattened_value)))
    else:
        items.append((parent_key, str(document)))
    return dict(items)

# Create MySQL table using DataFrame
def create_mysql_table(df, table_name, jdbc_url, user, password):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .mode("overwrite") \
        .save()

# Process data route
@app.route('/process_data', methods=['POST'])
def process_data():
    data = request.json
    print("Received data:", data)  # Log the received data

    # Check if MySQL connection details are in data
    if not all(key in data for key in ["mysql_jdbc_url", "mysql_user", "mysql_password"]):
        return jsonify({"status": "error", "message": "Missing MySQL connection details"}), 400
    mongo_uri = data["mongo_uri"]
    mysql_jdbc_url = data["mysql_jdbc_url"]
    mysql_user = data["mysql_user"]
    mysql_password = data["mysql_password"]
    table_name = data["table_name"]

    # MongoDB connection
    client = pymongo.MongoClient(mongo_uri)
    db = client.get_database()
    collection = db.get_collection(data["collection"])

    # Flatten MongoDB documents
    documents = collection.find()
    flattened_data = [flatten_document(doc) for doc in documents]

    # Convert to Spark DataFrame
    df = spark.createDataFrame(flattened_data)

    # Save to MySQL
    create_mysql_table(df, table_name, mysql_jdbc_url, mysql_user, mysql_password)

    return jsonify({"status": "success", "table": table_name})

if __name__ == '__main__':
    app.run(port=5000)
