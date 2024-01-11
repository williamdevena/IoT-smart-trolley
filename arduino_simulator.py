# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import time as t

import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT


def main() -> None:
    # Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE
    ENDPOINT = "a1o1h9paav6wpy-ats.iot.eu-west-2.amazonaws.com"
    CLIENT_ID = "testDevice"
    PATH_TO_CERTIFICATE = "certificates/certificate.pem"
    PATH_TO_PRIVATE_KEY = "certificates/privateKey.pem"
    PATH_TO_AMAZON_ROOT_CA_1 = "certificates/AmazonRootCA1.pem"
    MESSAGE = "Hello World"
    TOPIC = "arduino/outgoing"
    RANGE = 20

    myAWSIoTMQTTClient = AWSIoTPyMQTT.AWSIoTMQTTClient(CLIENT_ID)
    myAWSIoTMQTTClient.configureEndpoint(ENDPOINT, 8883)
    myAWSIoTMQTTClient.configureCredentials(PATH_TO_AMAZON_ROOT_CA_1, PATH_TO_PRIVATE_KEY, PATH_TO_CERTIFICATE)
    myAWSIoTMQTTClient.connect()

    print('Begin Publish')
    for i in range (RANGE):
        data = "{} [{}]".format(MESSAGE, i+1)
        message = {"message" : data}
        myAWSIoTMQTTClient.publish(TOPIC, json.dumps(message), 1)
        print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC)
        t.sleep(1)

    print('Publish End')
    myAWSIoTMQTTClient.disconnect()


if __name__=="__main__":
    main()