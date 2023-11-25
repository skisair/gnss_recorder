# GNSS Recorder

## MQTT Broker

https://www.rabbitmq.com/
MQTT Host : localhost
MQTT Port : 1883
MQTT TOPIC : devices/${device_id}/gnss/${data_id}


## Receiver

USB-Serial interface GNSS Receiver
device.py

- シリアルを読み込み保存
- シリアルの結果を解析し、JSONとして保存
- 解析結果をMQTTで送信

## Recorder

SQLite

- MQTTからメッセージを受信
- 受信結果をSQLiteに保存

## Viewer

Streamlit

- MQTTからメッセージを受信
- Pandasにデータを保存
- グラフを描画