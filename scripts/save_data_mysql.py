import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

def connect_mysql(host_name, user_name, pw):
    connection = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=pw
    )

    return connection

def create_cursor(cnx):
    return cnx.cursor()

def create_database(cursor, db_name):
    return cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")

def show_databases(cursor):
    return cursor.execute("SHOW DATABASES;")

def create_product_table(cursor, db_name, tb_name):
    return cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
            id VARCHAR(100),
            Produto VARCHAR(100),
            Categoria_Produto VARCHAR(100),
            Preco FLOAT(10, 2),
            Frete FLOAT(10, 2),
            Data_Compra DATE,
            Vendedor VARCHAR(100),
            Local_Compra VARCHAR(100),
            Avaliacao_Compra INT,
            Tipo_Pagamento VARCHAR(100),
            Qntd_Parcelas INT,
            Latitude FLOAT(10, 2),
            Longitude FLOAT(10, 2),
                
            PRIMARY KEY(id)
        );
    """)

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    return cursor.execute("SHOW TABLES;")

def read_csv(path):
    df = pd.read_csv(f"{path}")
    return df

def add_product(cnx, cursor, df, db_name, tb_name):
    data_list = [tuple(row) for i, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cursor.executemany(sql, data_list)
    cnx.commit()

# functions executions
try:
    # my sql connection
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    cnx = connect_mysql(mysql_host, mysql_user, mysql_password)
    cursor = create_cursor(cnx)
    # creating database
    db_name = "db_products_challenge"
    create_database(cursor, db_name)
    show_databases(cursor)
    for db in cursor:
        print(db)
    # creating table
    tb_name = "tb_products"
    create_product_table(cursor, db_name, tb_name)
    show_tables(cursor, db_name)
    for tb in cursor:
        print(tb)
    # adding data to table
    path = "/home/vanessaike/Estudos/pipeline-python-mongodb-mysql/data/table_products_from_2021.csv"
    df = read_csv(path)
    add_product(cnx, cursor, df, db_name, tb_name)
    print(cursor.rowcount, "inserted data")
    # closing connection
    cursor.close()
    cnx.close()
except Exception as e:
    print(e)