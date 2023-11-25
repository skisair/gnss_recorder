import os
import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get('MQTT_HOST', default='localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', default=1883))
MQTT_KEEP_ALIVE = int(os.environ.get('MQTT_KEEP_ALIVE', default=60))


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)


def on_message(client, userdata, msg):
    topics = msg.topic.split('/')
    device_id = topics[1]
    data_id = topics[3]
    payload = msg.payload.decode()
    print(f'device_id={device_id}, data_id={data_id}, payload={payload}')


if __name__ == '__main__':
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, port=MQTT_PORT, keepalive=MQTT_KEEP_ALIVE)
    client.subscribe('devices/+/gnss/+')
    client.loop_forever()