import json
import logging
import ssl
import sys
import time

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import costants, data_acquisition, data_storing, mqtt_comm


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


    client = mqtt_comm.configure_client(client_id=client_id,
                                        endpoint=endpoint,
                                        port=port,
                                        CA_path=CA_path,
                                        privateKey_path=privateKey_path,
                                        certificate_path=certificate_path)
    mqtt_comm.connect_client(client=client)

    message = "test message"
    mqtt_comm.publish_periodically(client, topic=incoming_topic, message="ciao", period=100, time_sleep=2)
    #mqtt_comm.publish_on_topic(client, incoming_topic, message)



if __name__=="__main__":
    main()