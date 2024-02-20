from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(collection):
    # print all documents in the collection
    for doc in collection.find():
        print(doc)

def rename_column(collection, col_name, new_name):
    collection.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(collection, category):
    # selects documents that correspond to a category
    query = {"Categoria do Produto": f"{category}"}
    books_list = []

    for doc in collection.find(query):
        books_list.append(doc)
    
    return books_list

def make_regex(collection, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    products_list = []
    for doc in collection.find(query):
        products_list.append(doc)

    return products_list

def create_dataframe(list):
    df = pd.DataFrame(list)
    return df

def format_date(df):
    # formats column date to "yyyy-mm-dd" format
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")
    return df

def save_csv(df, path):
    df.to_csv(f"{path}", index=False)

# functions executions
try:
    client = connect_mongo()
    db = create_connect_db(client, "test_products")
    collection = create_connect_collection(db, "products")
    # visualize_collection(collection)
    rename_column(collection, "lat", "Latitude")
    rename_column(collection, "lon", "Longitude")
    books = select_category(collection, "livros")
    products_from_2021 = make_regex(collection, "/202[1-9]")
    # creating df
    df_books = create_dataframe(books)
    df_products = create_dataframe(products_from_2021)
    # formatting date
    df_books = format_date(df_books)
    df_products = format_date(df_products)
    # saving as csv
    save_csv(df_books, "./data/challenge_books.csv")
    save_csv(df_products, "./data/challenge_products.csv")
    client.close()
except Exception as e:
    print(e)