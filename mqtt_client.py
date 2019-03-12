import os
import time
import paho.mqtt.client as mqtt

class MQTT:
    def __init__(self, hostname, client_name, topic, port=1883):
        self.hostname = hostname
        self.client_name = client_name
        self.topic = topic
        self.port = int(port)

        try:
            self.mqtt_client = mqtt.Client(client_id=self.client_name)
            print("Initialized MQTT Client with the following client_id: {0}".format(client_name) )
            self.mqtt_client.connect(host=self.hostname, port=self.port)
            print("Performed connection to MQTT broker on : {0} on port : {1}".format(hostname,port) )
            self.mqtt_client.on_message = self.on_message
            print("Initialized the callback ")
            self.mqtt_client.loop_start()
        except ConnectionRefusedError as error:
            print("Check if mosquitto was initialized error is {0}".format(error) )
            exit(-1)
        except Exception as eror:
            print("Error occured {0}".format(eror) )
            exit(-99)

    def on_message(self, client, userdata, message):
        print("Received message:[{0}] on topic:[{1}] qos:[{2}] retain flag:[{3}]".format(str(message.payload.decode("utf-8")), 
                                                                                             message.topic, 
                                                                                             message.qos, 
                                                                                             message.retain) )
    def publish(self, topic_name, payload):
        self.mqtt_client.publish(topic_name, payload=payload, qos=0, retain=False)
        print("Publishing message to broker on topic:[{0}] with the following payload {1}".format(topic,payload) )

    def subscribe(self, topic_name):
        self.mqtt_client.subscribe(topic=topic_name, qos=0)
        print("Client has been subscribed on topic:[{0}]".format(topic_name))

instance = MQTT("127.0.0.1", "MQTT_CLIENT_1", "IOT_TOPIC", 1883)
instance.subscribe("IOT_TOPIC")
while(True):
    instance.publish("IOT_TOPIC", "HELLO IOT")
    time.sleep(1)
