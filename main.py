import json
import logging
import ssl
import sys
import threading
import time

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import auth, costants, data_acquisition, data_storing, mqtt_comm


def main() -> None:
    """
    Turns on the back end of the project
    """
    client_id = "back-end"
    endpoint = "a1o1h9paav6wpy-ats.iot.eu-west-2.amazonaws.com"
    port = 8883
    CA_path = "./certificates/AmazonRootCA1.pem"
    privateKey_path = "./certificates/privateKey.pem"
    certificate_path = "./certificates/certificate.pem"
    incoming_topic = "arduino/outgoing"
    outgoing_topic = "backend/outgoing"

    aws_client = mqtt_comm.configure_client(client_id=client_id,
                                        endpoint=endpoint,
                                        port=port,
                                        CA_path=CA_path,
                                        privateKey_path=privateKey_path,
                                        certificate_path=certificate_path)
    mqtt_comm.connect_client(client=aws_client)

    #### SUBSCRIBER
    #### It subscribes to the topic "arduino/outgoing" (whee the arduino publishes)
    #### and when he receives a message from the arduino it automatically publish
    #### a message on the topic "backend/outgoing" so that when we have the connection
    #### to the DB it can query the DB a send back the result to the arduino.
    mqtt_comm.subscribe_to_topic(client=aws_client,
                                topic=incoming_topic,
                                callback_function=mqtt_comm.handle_arduino_message)




if __name__=="__main__":
    main()


