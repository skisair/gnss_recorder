import os
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
import platform
from string import Template
import uuid

import serial
import paho.mqtt.client as mqtt

from gnss_parser import GNSSParser

LOG_LEVEL = os.environ.get('LOG_LEVEL', default=logging.INFO)
DEVICE_ID = os.environ.get('DEVICE_ID', default=platform.uname()[1])

JSON_OUTPUT_FOLDER = os.environ.get('JSON_OUTPUT_FOLDER', default='data/${device_id}')
JSON_OUTPUT_FOLDER_FORMAT = os.environ.get('JSON_OUTPUT_FOLDER_FORMAT', default='%Y/%m/%d/%H')
JSON_OUTPUT_FILE_FORMAT = os.environ.get('JSON_OUTPUT_FILE_FORMAT', default='%Y%m%d%H%M%S%f-${data_id}-${id}.json')

SERIAL_OUTPUT_FOLDER = os.environ.get('SERIAL_OUTPUT_FOLDER', default='log/${device_id}/serial/%Y/%m/%d/%H')
SERIAL_OUTPUT_FILE_FORMAT = os.environ.get('SERIAL_OUTPUT_FILE_FORMAT', default='serial_${device_id}_%Y%m%d%H%M%S.log')

MQTT_HOST = os.environ.get('MQTT_HOST', default='localhost')
MQTT_PORT = int(os.environ.get('MQTT_PORT', default=1883))
MQTT_TOPIC = os.environ.get('MQTT_TOPIC', default='devices/${device_id}/gnss/${data_id}')
MQTT_KEEP_ALIVE = int(os.environ.get('MQTT_KEEP_ALIVE', default=60))

JST = timezone(timedelta(hours=+9), 'JST')
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
streamHandler = logging.StreamHandler()
logger.addHandler(streamHandler)


class GNSSReceiver:

    def __init__(self,
                 port=None,
                 baudrate=9600,
                 bytesize=serial.EIGHTBITS,
                 parity=serial.PARITY_NONE,
                 stopbits=serial.STOPBITS_ONE,
                 timeout=None,
                 xonxoff=False,
                 rtscts=False,
                 write_timeout=None,
                 dsrdtr=False,
                 inter_byte_timeout=None,
                 exclusive=None,):

        self.serial_port = serial.Serial(
            port=port,
            baudrate=baudrate,
            parity=parity,
            bytesize=bytesize,
            stopbits=stopbits,
            timeout=timeout,
            xonxoff=xonxoff,
            rtscts=rtscts,
            write_timeout=write_timeout,
            dsrdtr=dsrdtr,
            inter_byte_timeout=inter_byte_timeout,
            exclusive=exclusive,
        )
        self.running = True
        self.parser = GNSSParser()
        self.device_id = DEVICE_ID

        self.launch_time = datetime.now(JST)
    
        # シリアル入力の保存関連
        serial_output_folder = self.launch_time.strftime(
            Template(SERIAL_OUTPUT_FOLDER).substitute(device_id=self.device_id, **os.environ))
        os.makedirs(serial_output_folder, exist_ok=True)
        file_name = self.launch_time.strftime(
            Template(SERIAL_OUTPUT_FILE_FORMAT).substitute(device_id=self.device_id, **os.environ))
        self.serial_output_file = open(file=os.path.join(serial_output_folder, file_name), mode='w', encoding='UTF-8')
        
        # JSON出力の保存関連
        self.json_output_folder = self.launch_time.strftime(
            Template(JSON_OUTPUT_FOLDER).substitute(device_id=self.device_id, **os.environ))
        os.makedirs(self.json_output_folder, exist_ok=True)

        # MQTT関連
        self.mqtt_host = MQTT_HOST
        self.mqtt_topic = MQTT_TOPIC
        self.mqtt_port = MQTT_PORT
        self.mqtt_keep_alive = MQTT_KEEP_ALIVE

        self.client = mqtt.Client(protocol=mqtt.MQTTv311)
        self.client.connect(self.mqtt_host, port=self.mqtt_port, keepalive=self.mqtt_keep_alive)

    def __del__(self):
        """
        デストロイヤ
        :return:
        """
        self.serial_output_file.close()
        self.client.disconnect()

    def write_serial(self, local_time, gnss_raw_data):
        """
        シリアル受信結果の保存
        :param local_time:
        :param gnss_raw_data:
        :return:
        """
        try:
            self.serial_output_file.write(f'{local_time} {gnss_raw_data}\n')
        except Exception as e:
            logger.error('write_serial error:', e)

    def write_json(self, local_time, message):
        """
        メッセージの出力（フォルダにJSONで保存）
        :param local_time:
        :param message:
        :return:
        """
        unique_id = str(uuid.uuid4())
        output_string = json.dumps(message)
        if 'data_id' in message:
            data_id = message['data_id']
        else:
            data_id = '____'
        file_name = local_time.strftime(
            Template(JSON_OUTPUT_FILE_FORMAT).substitute(
                id=unique_id, device_id=self.device_id, data_id=data_id, **os.environ
            ))
        folder = local_time.strftime(
            Template(JSON_OUTPUT_FOLDER_FORMAT).substitute(
                id=unique_id, device_id=self.device_id, data_id=data_id, **os.environ
            ))
        folder = os.path.join(self.json_output_folder, folder)
        try:
            os.makedirs(folder, exist_ok=True)
            with open(file=os.path.join(folder, file_name), mode='w', encoding='UTF-8') as f:
                f.write(output_string)
        except Exception as e:
            logger.error('write_json error:', e)

    def send_mqtt(self, message):
        """
        MQTTに送信
        :param message:
        :return:
        """
        data_id = message['data_id']
        topic = Template(self.mqtt_topic).substitute(device_id=self.device_id, data_id=data_id, **os.environ)
        output_string = json.dumps(message)
        try:
            self.client.publish(topic, output_string)
        except Exception as e:
            logger.error('send_mqtt error:', e)

    def run(self):
        """
        メインループ
        ・シリアルからの読み込み
        ・データの解析
        ・データの保存
        ・データの送信
        :return:
        """
        while self.running:
            gnss_raw_data = self.serial_port.readline().decode('utf-8')
            gnss_raw_data = gnss_raw_data.replace('\x00', '').strip()
            if len(gnss_raw_data) == 0:
                continue
            local_time = datetime.now(JST).strftime('%Y%m%d%H%M%S%f')
            try:
                messages = self.parser.parse(gnss_raw_data)
                for message in messages:
                    message['device_id'] = self.device_id
                    message['local_time'] = local_time
                    self.write_json(local_time, message)
                    self.send_mqtt(message)
                    logger.debug(message)
            except ValueError as e:
                logger.error('Value error in parse gnss data', e)
