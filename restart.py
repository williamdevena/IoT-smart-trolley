from src import mqtt_comm


def send_restart_command():
    """
    Publishes a STOP message on the MQTT broker, that is going
    to be read by the Arduino.

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

    mqtt_comm.publish_on_topic(client=aws_client,
                    topic="backend/stop",
                    message="RESTART")



def main():
    send_restart_command()



if __name__=="__main__":
    main()