import json
import ssl
import sys
import time
from functools import partial
from typing import Dict

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import costants, data_acquisition, data_storing


def configure_client(client_id: str,
                     endpoint: str,
                     port: int,
                     CA_path: str,
                     privateKey_path: str,
                     certificate_path: str) -> AWSIoTMQTTClient:
    """
    Configures the settings of the AWS MQTT broker.

    Args:
        client_id (str): Client ID for the MQTT client.
        endpoint (str): AWS IoT endpoint.
        port (int): Port number for the MQTT connection.
        CA_path (str): Path to the CA certificate file.
        privateKey_path (str): Path to the private key file.
        certificate_path (str): Path to the client certificate file.

    Returns:
        AWSIoTMQTTClient: Configured MQTT client.
    """
    client = AWSIoTMQTTClient(client_id)
    client.configureEndpoint(endpoint, port)
    client.configureCredentials(CA_path, privateKey_path, certificate_path)

    return client


def connect_client(client: AWSIoTMQTTClient) -> None:
    """
    Connects to the AWS MQTT broker using the provided client.

    Args:
        client (AWSIoTMQTTClient): Configured MQTT client.

    Returns:
        None
    """
    client.connect()
    print("Client Connected")


def subscribe_to_topic(client: AWSIoTMQTTClient,
                       topic: str) -> None:
    """
    Subscribes the client to a specified MQTT topic and registers a callback function.

    Args:
        client (AWSIoTMQTTClient): MQTT client.
        topic (str): MQTT topic to subscribe to.

    Returns:
        None
    """
    client.subscribe(topic , 1, callback=partial(handle_arduino_message,
                                                aws_client=client,
                                                topic=topic))
    print(f"Subscribed to {topic} topic")
    print("Press any key to stop subscription")
    print("Waiting for incoming messages...\n")
    x = input()




def handle_arduino_message(message, aws_client: AWSIoTMQTTClient, topic: str) -> None:
    """
    Handles incoming messages from Arduino, processing them based on the topic.

    Args:
        message: The message payload.
        aws_client (AWSIoTMQTTClient): MQTT client for response.
        topic (str): Topic the message was received on.

    Returns:
        None
    """
    if topic=="arduino/product" or topic=="arduino/outgoing":
        handle_product_scanned(message=message, aws_client=aws_client)
    elif topic=="arduino/checkout":
        handle_checkout(message=message.payload, aws_client=aws_client)
    else:
        print(f"Arriving on topic {topic}")




def handle_checkout(message: bytes) -> None:
    """
    Handles checkout messages sent by Arduino.

    Args:
        message (bytes): Checkout message payload.

    Returns:
        None
    """
    checkout_dict = parse_checkout_string(message)
    data_storing.store_dict_into_mongodb(costants.CLUSTER_NAME,
                                        costants.DATABASE_NAME,
                                        costants.COLLECTION_CHECKOUTS,
                                        checkout_dict)



def parse_checkout_string(message_payload: str) -> Dict:
    """
    Parses the checkout information string received from Arduino.

    Args:
        message_payload (str): Checkout information string.

    Returns:
        Dict: Parsed checkout information including the amount and products list.
    """
    split = message_payload.split(" ")
    amount = float(split[0])
    products = split[1:]

    checkout_dict = {
        'amount': amount,
        'products': products
    }

    return checkout_dict


def handle_product_scanned(message, aws_client: AWSIoTMQTTClient) -> None:
    """
    Handles product scanned messages sent by Arduino.

    Args:
        message: Scanned product message payload.
        aws_client (AWSIoTMQTTClient): MQTT client to use for response.

    Returns:
        None
    """
    message_string = extract_message_from_payload(message_payload=message.payload)
    print("------")
    print(f"Topic: {message.topic}")
    print(f"Message: {message_string}\n")
    print(message_string)

    query_condition = {'code': message_string}
    data = data_acquisition.get_from_products(condition_product=query_condition)
    name, price = data_acquisition.get_product_info(data=data)
    publish_on_topic(client=aws_client,
                    topic="backend/outgoing",
                    message=f"\"name\":\"{name}\", \"price\":{price}")



def extract_message_from_payload(message_payload: bytes) -> str:
    """
    Extracts the message content from the MQTT message payload.

    Args:
        message_payload (bytes): Payload of the MQTT message.

    Returns:
        str: Extracted message content as a string.
    """
    encoding = 'utf-8'
    message = message_payload.decode(encoding)

    message = "".join(filter(str.isalnum, message))

    return message



def print_incoming_message(message) -> None:
    """
    Prints the payload of an incoming MQTT message.

    Args:
        message: Incoming MQTT message.

    Returns:
        None
    """
    print(f"Topic: {message.topic}")
    print(f"Message: {message.payload}\n")




def publish_periodically(client: AWSIoTMQTTClient,
                         topic: str,
                         message: str,
                         period: int,
                         time_sleep: int) -> None:
    """
    Publishes a message on a specified topic periodically.

    Args:
        client (AWSIoTMQTTClient): MQTT client to use for publishing.
        topic (str): to publish on a certain topic a certain message iteritavely.
    """
    for x in range(period):
        message = message + f" {x}"
        publish_on_topic(client, topic, message)
        time.sleep(time_sleep)


def publish_on_topic(client: AWSIoTMQTTClient, topic: str, message: str) -> None:
    """
    Publishes a given message on a specified MQTT topic once.

    Args:
        client (AWSIoTMQTTClient): MQTT client to use for publishing.
        topic (str): The topic on which the message will be published.
        message (str): The message to be published.

    Returns:
        None
    """
    # Encoding into JSON
    #client.json_encode=json_encode
    message = json.dumps(message)
    # Sending
    send(client=client, topic=topic, message=message)


def send(client: AWSIoTMQTTClient, topic: str, message: str) -> None:
    """
    Publishes a message on a specified MQTT topic.

    Args:
        client (AWSIoTMQTTClient): MQTT client to use for publishing.
        topic (str): The topic on which the message will be published.
        message (str): The message to be published.

    Returns:
        None
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