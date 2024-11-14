from pyspark.shell import spark
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pymysql
from pyspark.sql.functions import col, explode, struct

def extract_from_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["your_database_name"]
    collection = db["your_collection_name"]

    # Convert MongoDB documents to a Spark DataFrame
    df = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/your_database_name.your_collection_name") \
        .load()

    return df

def transform_data(df):
    # Flatten nested structures (adjust based on your specific schema)
    df = df.withColumn("flattened_array", explode(df["nested_array_column"])) \
           .select("*", col("flattened_array.*"))

    # Drop duplicate rows
    df = df.dropDuplicates()

    # Handle missing values and data type conversions
    df = df.fillna(value=-999, subset=["numerical_column"])
    df = df.withColumn("string_column", df["string_column"].cast("string").replace("", "NA", regex=True))

    return df

def load_to_datalake(df):
    df.write.format("parquet").save("path/to/datalake")

def load_to_mysql(df, table_name):
    conn = pymysql.connect(
        host="your_mysql_host",
        user="your_mysql_user",
        password="your_mysql_password",
        database="your_mysql_database"
    )

    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")

    table_exists = cursor.fetchone() is not None

    if not table_exists:
        # Create table schema based on DataFrame (adjust as needed)
        create_table_sql = f"""
            CREATE TABLE {table_name} (
                column1 INT,
                column2 VARCHAR(255),
                -- ... other columns based on your DataFrame schema
            )
        """
        cursor.execute(create_table_sql)

    # Write DataFrame to MySQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://your_mysql_host:3306/your_mysql_database") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "your_mysql_user") \
        .option("password", "your_mysql_password") \
        .mode("append") \
        .save()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    df = extract_from_mongodb()
    df = transform_data(df)
    load_to_datalake(df)
    load_to_mysql(df, "your_mysql_table_name")