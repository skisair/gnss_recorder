import os
import time
from string import Template
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import paho.mqtt.client as mqtt

JST = timezone(timedelta(hours=+9), 'JST')

MQTT_HOST = os.environ.get('MQTT_HOST', default='localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', default=1883))
MQTT_TOPIC = os.environ.get('MQTT_TOPIC', default='devices/${device_id}/gnss/${data_id}')
MQTT_KEEP_ALIVE = int(os.environ.get('MQTT_KEEP_ALIVE', default=60))


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)


if __name__ == '__main__':
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.connect(MQTT_HOST, port=MQTT_PORT, keepalive=MQTT_KEEP_ALIVE)

    mqtt_topic = Template(MQTT_TOPIC).substitute(device_id='TEST_DEVICE_ID', data_id='TEST_DATA_ID')

    for i in range(10):
        payload = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S.%f')
        client.publish(
            topic=mqtt_topic,
            payload=payload,
        )
        print(f'send topic={mqtt_topic} payload={payload}')
        time.sleep(1.0)

    client.disconnect()
