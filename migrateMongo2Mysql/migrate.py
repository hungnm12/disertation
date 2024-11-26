import re
import time
import logging
import json
import pymongo
import pymysql
import traceback
from datetime import datetime
from bson.objectid import ObjectId
from config import mysql_config, mysql_db_name

DEFAULT_VARCHAR_SIZE = 25
MAX_VARCHAR_LENGTH = 1000
SKIP_ID_FIELD = False
SKIP_CLASS_FIELD = False

def camel_to_snake(name):
    name = name.replace(' ', '_')
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def is_child_key_present_in_value(child_key, value_values):
    return child_key in value_values

def convert_document(document, prefix=''):
    return {f"{prefix}_{key}" if prefix else key: convert_value(value) for key, value in document.items()}

def enquote(identifier):
    return f"`{identifier}`"

def type_to_mysql(column_name, py_type, max_length):
    varchar_type = f'VARCHAR({min(max(DEFAULT_VARCHAR_SIZE, (max_length // DEFAULT_VARCHAR_SIZE + 1) * DEFAULT_VARCHAR_SIZE), MAX_VARCHAR_LENGTH)})' if max_length and max_length <= MAX_VARCHAR_LENGTH else 'TEXT'
    type_mapping = {
        'str': varchar_type,
        'int': 'INT',
        'float': 'FLOAT',
        'bool': 'BOOLEAN',
        'ObjectId': 'VARCHAR(24)',
        'datetime': 'DATETIME',
        'Int64': 'BIGINT',
        'Decimal128': 'DECIMAL(38, 3)',
        'list': 'TEXT',
        'bytes': 'BLOB',
        'NoneType': 'TEXT',
        're.Pattern': 'TEXT',
    }
    return type_mapping.get(py_type, 'VARCHAR(255)')

def process_nested_document(doc, prefix=''):
    structure = {}
    for child_key, value in doc.items():
        if isinstance(value, dict):
            value_values = value.values()
            if is_child_key_present_in_value(child_key, value_values):
                value_keys = value.keys()
                for inner_key in value_keys:
                    new_key = f"{prefix}_{camel_to_snake(inner_key)}" if prefix else camel_to_snake(inner_key)
                    inner_key_value = value[inner_key]
                    max_length = len(str(inner_key_value))
                    structure[new_key] = type_to_mysql(new_key, type(inner_key_value).__name__, max_length)
                    if isinstance(inner_key_value, dict):
                        structure.update(process_nested_document(inner_key_value, new_key))
            else:
                new_key = f"{prefix}_{camel_to_snake(child_key)}" if prefix else camel_to_snake(child_key)
                structure.update(process_nested_document(value, new_key))
        else:
            max_length = len(str(value))
            new_key = f"{prefix}_{camel_to_snake(child_key)}" if prefix else camel_to_snake(child_key)
            structure[new_key] = type_to_mysql(new_key, type(value).__name__, max_length)
    return structure

def convert_nested_document(doc, prefix=''):
    new_document = {}
    for child_key, doc_value in doc.items():
        if isinstance(doc_value, dict):
            value_values = doc_value.values()
            if is_child_key_present_in_value(child_key, value_values):
                value_keys = doc_value.keys()
                for inner_key in value_keys:
                    new_key = f"{prefix}_{camel_to_snake(inner_key)}" if prefix else camel_to_snake(inner_key)
                    inner_key_value = doc_value[inner_key]
                    new_document[new_key] = convert_value(inner_key_value)
                    if isinstance(inner_key_value, dict):
                        new_document.update(convert_nested_document(inner_key_value, new_key))
            else:
                new_key = f"{prefix}_{camel_to_snake(child_key)}" if prefix else camel_to_snake(child_key)
                new_document.update(convert_nested_document(doc_value, new_key))
        else:
            new_key = f"{prefix}_{camel_to_snake(child_key)}" if prefix else camel_to_snake(child_key)
            new_document[new_key] = convert_value(doc_value)
    return new_document

def convert_value(value):
    if isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, list):
        return json.dumps(value, default=str)
    elif value is None:
        return 'NULL'
    elif isinstance(value, bool):
        return 1 if value else 0
    else:
        return value

def create_mysql_table(mysql_cursor, collection_name, document):
    collection_name = camel_to_snake(collection_name)
    max_lengths = {camel_to_snake(key): len(str(value)) for key, value in document.items() if key not in ['_id', '_class']}
    structure = {}
    for key, value in document.items():
        if key not in ['_id', '_class'] or (key == '_id' and not SKIP_ID_FIELD) or (key == '_class' and not SKIP_CLASS_FIELD):
            if isinstance(value, dict):
                structure.update(process_nested_document(value, camel_to_snake(key)))
            else:
                structure[camel_to_snake(key)] = type_to_mysql(camel_to_snake(key), type(value).__name__, max_lengths.get(camel_to_snake(key)))

    column_definitions = []
    if "id" not in structure:
        column_definitions.append("id INT AUTO_INCREMENT PRIMARY KEY")
    column_definitions.extend([f'{enquote(key)} {structure[key]}' for key in structure.keys()])
    sql = f"CREATE TABLE IF NOT EXISTS {enquote(collection_name)} ({', '.join(column_definitions)});"
    mysql_cursor.execute(sql)

def insert_into_mysql(mysql_cursor, collection_name, document):
    collection_name = camel_to_snake(collection_name)
    document = {camel_to_snake(key): value for key, value in document.items() if key not in ['_id', '_class'] or (key == '_id' and not SKIP_ID_FIELD) or (key == '_class' and not SKIP_CLASS_FIELD)}
    document = convert_document(document)
    keys = ', '.join(enquote(key) for key in document.keys())
    values = ', '.join(['%s' for _ in document.values()])
    sql = f"INSERT INTO {enquote(collection_name)} ({keys}) VALUES ({values})"
    values_tuple = tuple(str(value) for value in document.values())
    quoted_values_tuple = tuple(f"'{value}'" if isinstance(value, str) else value for value in values_tuple)
    print(sql % quoted_values_tuple)
    while True:
        try:
            mysql_cursor.execute(sql, values_tuple)
            break
        except pymysql.err.OperationalError as e:
            if 'Unknown column' in str(e):
                missing_fields = re.findall(r"Unknown column '([^']+)'", str(e))
                for field in missing_fields:
                    field_length = len(str(document[field]))
                    mongo_type = type(document[field]).__name__
                    field_type = type_to_mysql(field, mongo_type, field_length)
                    mysql_cursor.execute(f"ALTER TABLE {collection_name} ADD COLUMN {field} {field_type}")
            elif 'Incorrect datetime value' in str(e):
                field = re.search(r"column '([^']+)'", str(e)).group(1)
                value = re.search(r"'([^']+)'", str(e)).group(1)
                value_length = len(value)
                varchar_size = min(max(DEFAULT_VARCHAR_SIZE, (value_length // DEFAULT_VARCHAR_SIZE + 1) * DEFAULT_VARCHAR_SIZE), MAX_VARCHAR_LENGTH)
                mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} VARCHAR({varchar_size})")
            else:
                raise
        except pymysql.err.DataError as e:
            if 'Data too long' in str(e):
                field = re.search(r"'(.+)'", str(e)).group(1)
                mysql_cursor.execute(f"SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{collection_name}' AND COLUMN_NAME = '{field}'")
                current_type, current_length = mysql_cursor.fetchone()
                if current_type == 'varchar':
                    new_length = current_length * 2
                    if new_length > MAX_VARCHAR_LENGTH:
                        new_type = 'TEXT'
                    else:
                        new_type = f'VARCHAR({new_length})'
                elif current_type == 'text':
                    new_type = 'MEDIUMTEXT'
                elif current_type == 'mediumtext':
                    new_type = 'LONGTEXT'
                else:
                    raise ValueError(f"Cannot increase size of field {field} of type {current_type}")
                mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} {new_type}")
            elif 'Data truncated' in str(e) or 'Incorrect integer value' in str(e):
                field = re.search(r"column '([^']+)'", str(e)).group(1)
                mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} VARCHAR({DEFAULT_VARCHAR_SIZE})")
            else:
                raise


logging.basicConfig(level=logging.INFO)


def run_migration(mongo_config):
    try:
        start_time = time.time()
        logging.info("Connecting to MongoDB...")

        # Connect to MongoDB
        mongo_client = pymongo.MongoClient(mongo_config['uri'])
        mongo_db = mongo_client[mongo_config['database']]
        mongo_collection = mongo_db[mongo_config['collection']]
        logging.info("Connected to MongoDB.")
        logging.debug(f"MongoDB Document Count: {mongo_collection.count_documents({})}")

        logging.info("Connecting to MySQL...")
        # Connect to MySQL
        mysql_connection = pymysql.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_db_name
        )
        mysql_cursor = mysql_connection.cursor()
        logging.info("Connected to MySQL.")

        # Migrate data from MongoDB to MySQL
        logging.info("Starting data migration...")
        for document in mongo_collection.find():
            logging.debug(f"Processing document: {document}")
            table_name = mongo_config['collection']
            create_mysql_table(mysql_cursor, table_name, document)
            insert_into_mysql(mysql_cursor, table_name, document)

        # Commit the transaction
        mysql_connection.commit()
        end_time = time.time()
        logging.info(f"Migration completed in {end_time - start_time:.2f} seconds.")

    except pymongo.errors.ConnectionError:
        logging.error("Failed to connect to MongoDB. Please check your MongoDB URI and credentials.")
    except pymysql.MySQLError as e:
        logging.error("MySQL error occurred:")
        logging.error(traceback.format_exc())
    except Exception as e:
        logging.error("An unexpected error occurred during migration:")
        logging.error(traceback.format_exc())
    finally:
        # Close MongoDB connection
        mongo_client.close()
        logging.info("Closed MongoDB connection.")

        # Close MySQL connection
        mysql_cursor.close()
        mysql_connection.close()
        logging.info("Closed MySQL connection.")