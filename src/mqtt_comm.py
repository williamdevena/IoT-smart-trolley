import json
import ssl
import sys
import time
from functools import partial

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import costants, data_acquisition, data_storing


def configure_client(client_id, endpoint, port, CA_path, privateKey_path, certificate_path):
    """
    Configures the settings of the AWS MQTT broker.
    """
    client = AWSIoTMQTTClient(client_id)
    client.configureEndpoint(endpoint, port)
    client.configureCredentials(CA_path, privateKey_path, certificate_path)

    return client


def connect_client(client):
    """
    Connects to the AWS MQTT broker.
    """
    client.connect()
    print("Client Connected")


def subscribe_to_topic(client, topic, callback_function):
    """
    Subscribes to a certain topic and waits for incoming messages.
    """
    client.subscribe(topic , 1, callback=partial(handle_arduino_message,
                                                aws_client=client,
                                                topic=topic))
    print(f"Subscribed to {topic} topic")
    print("Press any key to stop subscription")
    print("Waiting for incoming messages...\n")
    x = input()




def handle_arduino_message(client, userdata, message, aws_client, topic):
    """
    It handles an arrived message from the Arduino: it querys the MongoDB
    database to find the product with string published by the Arduino on
    the MQTT broker.AWSIoTMQTTClient

    """
    print("---------")
    if topic=="arduino/product" or topic=="arduino/outgoing":
        handle_product_scanned(message=message, aws_client=aws_client)
    elif topic=="arduino/checkout":
        handle_checkout(message=message.payload, aws_client=aws_client)
    else:
        print(f"Arriving on topic {topic}")




def handle_checkout(message, aws_client):
    """
    Handles an arrived checkout message sent by the Arduino.

    Args:
        - message (bytes): payload of the message sent by the Arduino.
        - aws_client (Client): mqtt client

    Returns: None
    """

    checkout_dict = parse_checkout_string(message)
    #data_dict = {'name':'Pizza', 'code':'4B00B82BC31B','price':'5.99'}
    data_storing.store_dict_into_mongodb(costants.CLUSTER_NAME,
                                        costants.DATABASE_NAME,
                                        costants.COLLECTION_CHECKOUTS,
                                        checkout_dict)



def parse_checkout_string(message_payload):
    """
    Parses the string published by the Arduino on the chekout event.

    Args:
        - message_payload (str): contains the checkout info sent by
        the arduino.AWSIoTMQTTClient

    Returns:
        - checkout_dict (Dict): contains the parsed info
    """
    split = message_payload.split(" ")
    amount = float(split[0])
    products = split[1:]

    checkout_dict = {
        'amount': amount,
        'products': products
    }

    return checkout_dict


def handle_product_scanned(message, aws_client):
    """
    Handles an arrived product scanned message sent by the Arduino.

    Args:
        - message (bytes): payload of the message sent by the Arduino.
        - aws_client (Client): mqtt client

    Returns
    """
    message_string = extract_message_from_payload(message_payload=message.payload)
    print("------")
    print(f"Topic: {message.topic}")
    print(f"Message: {message_string}\n")
    print(message_string)
    # data_dict = {'name': 'Bread', 'code': message_string, 'price':'2.99'}
    # data_storing.store_dict_into_mongodb(costants.CLUSTER_NAME, costants.DATABASE_NAME, costants.COLLECTION_PRODUCTS, data_dict)
    query_condition = {'code': message_string}
    data = data_acquisition.get_from_products(condition_product=query_condition)
    name, price = data_acquisition.get_product_info(data=data)
    publish_on_topic(client=aws_client,
                    topic="backend/outgoing",
                    message=f"\"name\":\"{name}\", \"price\":{price}")



def extract_message_from_payload(message_payload):
    """
    Extracts the message string from the payload read from the MQTT
    broker.

    Args:
        - message_payload (bytes): contains the message published on
        the MQTT broker

    Returns:
        - message (str): message in the form of a string
    """
    encoding = 'utf-8'
    message = message_payload.decode(encoding)

    message = "".join(filter(str.isalnum, message))

    return message



def print_incoming_message(client, userdata, message):
    """
    Prints the payload of an incoming message.
    """
    print(f"Topic: {message.topic}")
    print(f"Message: {message.payload}\n")
    #print(f"User data: {userdata}")




def publish_periodically(client, topic, message, period, time_sleep):
    """
    Used to publish on a certain topic a certain message iteritavely.
    """
    for x in range(period):
        message = message + f" {x}"
        publish_on_topic(client, topic, message)
        time.sleep(time_sleep)


def publish_on_topic(client, topic, message):
    """
    Used to publish on a certain topic a certain message once.
    """
    # Encoding into JSON
    #client.json_encode=json_encode
    message = json.dumps(message)
    # Sending
    send(client=client, topic=topic, message=message)


def send(client, topic, message):
    """
    Used to publish on a certain topic a certain message iteritavely.
    """
    #client.publish(topic, message, 1)
    client.publishAsync(topic, message, 1)
    print(f"Message Published on {topic}")


def main():
    checkout = "345.6 sdbcsd6vdsfcjs sdncsbdcsd77777 bbbbb88888"
    price, products = parse_checkout_string(checkout)
    print(price)
    print(products)


if __name__=="__main__":
    main()