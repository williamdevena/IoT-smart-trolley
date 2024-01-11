"""
This module contains the functions to store data both locally and
remotely into a MongoDB database
"""

import logging
import os

#import pandas as pd
import pymongo

from src import auth, costants


def store_dict_into_mongodb(cluster_name, database_name, collection_name, data_dict):
    """
    Inserts a pandas dataframe into a MongoDb database in the form of a collection

    Args:
        - cluster_name (str): Name of the cluster
        - database_name (str): Name of the database
        - collection_name (str): Name of the collection
        - df (pandas.core.frame.DataFrame): dataframe we want to insert into the
        database as a collection

    Returns: None
    """
    data_dict = [data_dict]
    store_collection_into_db(
        cluster_name=cluster_name, database_name=database_name, collection_name=collection_name, data=data_dict
    )


def store_collection_into_db(
    cluster_name, database_name, collection_name, data
):
    """
    Inserts a list of MongoDB documents (dictionaries) into a specific collection of a database of a cluster.

    Args:
        - cluster_name (str): Name of the cluster
        - database_name (str): Name of the database
        - collection_name (str): Name of the collection
        - data (List): List of dictionaries, where every dictionary represents a row (document) in the collection

    Returns:
        - None
    """
    client = connect_cluster_mongodb(
        cluster_name, auth.MONGODB_USERNAME, auth.MONGODB_PASSWORD
    )
    database = connect_database(client, database_name)
    collection, collection_already_exists = connect_collection(
        database, collection_name)
    collection.insert_many(data)
    logging.info(
        f"- STORED '{collection_name}' REMOTELY IN THE {database_name} DATABASE")


def connect_cluster_mongodb(cluster_name, username, password):
    """
    Opens a connection with a MongoDB cluster

    Args:
        - cluster_name (str): name of the cluster
        - username (str): username used ofr authentication
        - password (str): password used for authentication

    Returns:
        - client (MongoClient): client we use to comunicate with the database
    """
    connection_string = f"mongodb://{username}:{password}@ac-sh3a7ys-shard-00-00.rjmwtmn.mongodb.net:27017,ac-sh3a7ys-shard-00-01.rjmwtmn.mongodb.net:27017,ac-sh3a7ys-shard-00-02.rjmwtmn.mongodb.net:27017/?ssl=true&replicaSet=atlas-n4ij5c-shard-0&authSource=admin&retryWrites=true&w=majority"
    client = pymongo.MongoClient(connection_string)

    return client


def connect_database(client, database_name):
    """
    Returns a specific database of a MongoDB cluster

    Args:
        - client (MongoClient): client object
        - database_name (str): name of the databse we want to connect to

    Returns:
        - database (MongoDatabase): database object
    """
    # If databse doen't exist in the cluster it creates automatically
    if database_name not in client.list_database_names():
        logging.info(
            f"- The '{database_name}' database doesn't exist so MongoDB is going to create it automatically."
        )
    database = client[database_name]

    return database


def connect_collection(database, collection_name):
    """
    Returns a specific collection of a MongoDB database

    Args:
        - database (pymongo.database.Database): database object
        - collection_name (str): name of the collection we want to connect to

    Returns:
        - collection (pymongo.collection.Collection): collection object
        - collection_already_exists (bool): indicates if the collection already
        existed into the database
    """
    collection_already_exists = False
    # If collection doen't exist in the database it creates automatically
    if collection_name not in database.list_collection_names():
        logging.info(
            f"- The '{collection_name}' collection doesn't exist in the {database.name} database so MongoDB is going to create it automatically."
        )
    else:
        collection_already_exists = True

    collection = database[collection_name]

    return collection, collection_already_exists


def main():
    # cluster = "daps2022"
    # database = "hackathon_DAPS"
    # collection = "google_jax_commits"

    pass


if __name__ == "__main__":
    main()
