"""
This module contains the functions used to acquire data both locally and remotely
"""

import json
import logging
from datetime import datetime

import pandas as pd

from src import auth, costants, data_storing

#import talib
#from covid19dh import covid19





def get_product_info(data):
    """
    Extracts the needed product information from the data
    returned by the MongoDB database.

    Args:
        - data (List): contains the result of the query

    Returns:
        - name (str): name of the product
        - price (float): price of the product

    """
    print(data)
    product = data[0]
    name = product['name']
    price = float(product['price'])

    return name, price


def get_from_products(condition_product={}):
    """
    Querys the MongoDB database for a certain product.

    Args:
        - condition_product (Dict): conditions of the query

    Returns: None

    """
    database = costants.DATABASE_NAME
    collection = costants.COLLECTION_PRODUCTS
    data = []

    logging.info(
        f"\n- Acquiring '{collection}' from the MongoDB '{database}' database")
    collection_data = read_mongodb_collection(
        cluster_name=costants.CLUSTER_NAME, database_name=database,
        collection_name=collection, condition=condition_product
    )
    for doc in collection_data:
        data.append(doc)

    return data


def acquire_from_database():
    """
    Acquires all the data (covid data, stock data, technical data, ...)
    from the MongoDB database.

    Returns:
        - data (List) : contains all the data of the data acquisition stage
        in the form of pd.Dataframe(s)
    """
    database = costants.DATABASE_NAME
    data = []

    for collection in costants.COLLECTIONORDER:
        logging.info(
            f"\n- Acquiring '{collection}' from the MongoDB '{database}' database")
        collection_data = read_mongodb_collection(
            cluster_name=costants.CLUSTER_NAME, database_name=database,
            collection_name=collection
        )
        #print(collection_data)
        for doc in collection_data:
            data.append(doc)

    return data




def read_mongodb_collection(cluster_name, database_name, collection_name, condition={}, projection={}):
    """
    Reads from a MongoDB database a certain collection and if given querys with certain conditions.

    Args:
        - cluster_name (str): Name of the MongoDB cluster
        - database_name (str): Name of the MongoDB database
        - collection_name (str): Name of the MongoDB collection
        - condition (dict): Dictionary containing the conditions of the query.
        (EX: condition = {'name' : 'William'} gets all the documents of the collection
        that have 'name'='William')

    Returns:
        - (pymongo.cursor.Cursor): A pymongo Cursor object that is iterable and that
        represents the result of the query.
    """
    client = data_storing.connect_cluster_mongodb(
        cluster_name, auth.MONGODB_USERNAME, auth.MONGODB_PASSWORD)
    database = data_storing.connect_database(client, database_name)
    collection = data_storing.connect_collection(database, collection_name)[0]
    # logging.info(
    #     f"\n- Reading the '{collection_name}' collection in the '{database_name}' database")

    return collection.find(condition, projection)


def main():
    # start_date = datetime(2017, 4, 1)
    # end_date = datetime(2022, 4, 30)

    # data = download_stock_data(costants.AAPL, start_date, end_date)
    # plt.plot(data)
    # plt.show()
    pass


if __name__ == "__main__":
    main()
