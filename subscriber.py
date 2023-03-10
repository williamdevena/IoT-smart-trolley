import json
import logging
import ssl
import sys
import time

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

from src import costants, data_acquisition, data_storing, mqtt_comm


def main():
    """
    Turns on the back end of the project
    """


    # logging.basicConfig(
    #     level=logging.INFO,
    #     format="%(message)s",
    # )

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

    mqtt_comm.subscribe_to_topic(client=client,
                                topic=outgoing_topic,
                                callback_function=mqtt_comm.print_incoming_message)




if __name__=="__main__":
    main()