 import pyodbc
 import os


def warehouse_connection():
  server_name = os.getenv('s_name')
  database_name = os.getenv('db_name')
  username = os.getenv('username')
  password = os.getenv('pwd')
  driver = os.getenv('dvr')


  create_database_query = f"CREATE DATABASE YourDatabase"
  create_schema_query = f"CREATE SCHEMA YourSchema"

  table_queries = ["""CREATE TABLE IF NOT EXISTS ADOPTION(
           recorded_name VARCHAR,
    date_id INT FOREIGN KEY,
    outcome_id INT FOREIGN KEY,
    Animal_id INT FOREIGN KEY,
    Sex VARCHAR,
    Breed_id INT FOREIGN KEY,
    Color VARCHAR,
    ID VARCHAR
    );""",

    """CREATE TABLE IF NOT EXISTS OUTCOME(
    outcome VARCHAR,
    outcome_id INT PRIMARY KEY
    );""",

    """CREATE TABLE IF NOT EXISTS ANIMAL(
    Animal VARCHAR,
    Animal_id INT PRIMARY KEY
    );""",

    """CREATE TABLE IF NOT EXISTS BREED(
    Breed VARCHAR,
    Breed_id INT PRIMARY KEY
    );""",

    """CREATE TABLE IF NOT EXISTS DATE(
    Dt timestamp,
    date_id INT PRIMARY KEY,
    Mnt INT,
    Yr INT
    );"""
    ]

  connection_string = f"DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"

  connection = pyodbc.connect(connection_string)
  cursor = connection.cursor()

  try:
    cursor.execute(create_database_query)
    cursor.execute(create_schema_query)
    for query in table_queries:
      cursor.execute(query)

  except pyodbc.Error as ex:
    print("Error:", ex)
  finally:
    cursor.close()
    connection.close()