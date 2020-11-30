import paho.mqtt.client as mqtt
import os
import time

broker=os.environ.get('K3S_SERVICE_BROKER','10.0.1.8')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
        if rc==0:
            client.connected_flag=True #set flag

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("hello")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    
def connection():
        client.connect(broker,1883,60)
        client.loop_start()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
mqtt.Client.connected_flag=False

connection()


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
