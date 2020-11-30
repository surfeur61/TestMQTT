import paho.mqtt.client as mqtt
import os
import time
import requests

hostname=os.environ.get('HOSTNAME','sensor-0')
broker=os.environ.get('K3S_SERVICE_BROKER','10.0.1.8')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

        if rc==0:
                client.connected_flag=True #set flag

def on_disconnect(client, userdata,rc):
        connection()

def connection():
        c=0
        while not client.connected_flag:
                try:
                        client.connect(broker[c],1883,60)
                        client.loop_start()
                        while client.connected_flag==False:
                                time.sleep(3)
                except:
                        c+=1
                        pass

                if c==len(broker):
                        c=0

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
mqtt.Client.connected_flag=False
result = None

if (hostname=='sensor-0'):
        result=requests.get("https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.nom_dept=Rh%C3%B4ne")
elif (hostname=='sensor-1'):
        result=requests.get("https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.nom_dept=Rh%C3%B4ne")
elif (hostname=='sensor-2'):
        result=requests.get("https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.nom_dept=Rh%C3%B4ne")
elif (hostname=='sensor-3'):
        result=requests.get("https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.nom_dept=Rh%C3%B4ne")
else :
        result=requests.get("https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.nom_dept=Rh%C3%B4ne")

connection()

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.publish("hello", result.content)
client.loop_stop()
client.disconnect()
