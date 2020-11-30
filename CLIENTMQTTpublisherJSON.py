import paho.mqtt.client as mqtt
import os
import time
import requests
import json

hostname=os.environ.get('HOSTNAME','sensor-0')
broker=os.environ.get('K3S_SERVICE_BROKER','10.0.1.8')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

        if rc==0:
                client.connected_flag=True #set flag

def on_disconnect(client, userdata,rc):
        connection()

def connection():
        client.connect(broker,1883,60)
        client.loop_start()

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
result = None


with open('data.json') as json_data:
    data_dict = json.load(json_data)

data_str = json.dumps(data_dict)

connection()

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.publish("hello", data_str)
client.loop_stop()
client.disconnect()
