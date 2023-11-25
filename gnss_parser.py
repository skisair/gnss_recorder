import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
from typing import List, Dict

TARGET_DATA_ID = os.environ.get('TARGET_DATA_ID', default='GPRMC,GPGGA,GPVTG,GPGSA,GPGSV,GPGLL,GPTXT')
EXCLUDE_DATA_ID = os.environ.get('EXCLUDE_DATA_ID', default='')

LOG_LEVEL = os.environ.get('LOG_LEVEL', default=logging.INFO)

JST = timezone(timedelta(hours=+9), 'JST')
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
streamHandler = logging.StreamHandler()
logger.addHandler(streamHandler)


class GNSSParser:

    def __init__(self):
        self.target_data_ids = TARGET_DATA_ID.split(',')
        self.exclude_data_ids = EXCLUDE_DATA_ID.split(',')

    def parse(self, gnss_raw_data: str) -> List[Dict]:
        """
        データIDに基づく解析処理の分岐
        :param gnss_data: GNSSメッセージを,で分割したもの
        :return:
        """
        result = []

        gnss_data, check_sum = gnss_raw_data.split('*')
        gnss_data = gnss_data.split(',')

        data_id = gnss_data[0][1:]
        if data_id not in self.target_data_ids:
            logger.debug(f'{data_id}: not in target_data_ids:{self.target_data_ids}')
            return result
        elif data_id in self.exclude_data_ids:
            logger.debug(f'{data_id}: in exclude_data_ids:{self.exclude_data_ids}')
            return result

        try:
            if data_id == 'GNRMC':
                self._parse_GNRMC(data_id, gnss_data, result)
            elif data_id == 'GPRMC':
                self._parse_GPRMC(data_id, gnss_data, result)
            elif data_id == 'GPGGA':
                self._parse_GPGGA(data_id, gnss_data, result)
            elif data_id == 'GNVTG':
                self._parse_GPVTG(data_id, gnss_data, result)
            elif data_id == 'GPVTG':
                self._parse_GPVTG(data_id, gnss_data, result)
            elif data_id == 'GPGSA':
                self._parse_GPGSA(data_id, gnss_data, result)
            elif data_id == 'GNGSA':
                self._parse_GPGSA(data_id, gnss_data, result)
            elif data_id == 'GNGSV':
                self._parse_GPGSV(data_id, gnss_data, result)
            elif data_id == 'GPGSV':
                self._parse_GPGSV(data_id, gnss_data, result)
            elif data_id == 'GNGLL':
                self._parse_GPGLL(data_id, gnss_data, result)
            elif data_id == 'GPGLL':
                self._parse_GPGLL(data_id, gnss_data, result)
            elif data_id == 'GPTXT':
                logger.info(f'message from device : {" ".join(gnss_data)}')
            elif data_id == 'GNTXT':
                logger.info(f'message from device : {" ".join(gnss_data)}')
            else:
                logger.warning(f'data_id {data_id} is not supported : {gnss_data}')
        except Exception as e:
            logger.error(f'parse error in data_id:{data_id} values:{gnss_data} : {e}')

        return result

    def _parse_GPGLL(self, data_id, values, result):
        """
        GPGLLの解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        utc_date = datetime.utcnow().strftime('%d%m%y')
        utc_time = values[5]
        try:
            gps_date_time = datetime.strptime(utc_date + utc_time, '%d%m%y%H%M%S.%f').isoformat()
        except ValueError:
            logger.warning(f'date format error in GPRMC {values}')
            gps_date_time = datetime.now().isoformat()

        warning = values[6]
        if warning == 'V':
            logger.warning(f'GPGGA staus is V.')
        else:
            lat = self.parse_matrix_value(values[1])
            lon = self.parse_matrix_value(values[3])
            lat_d = values[2]
            lon_d = values[4]
            if lat_d == 'S':
                lat = -lat
            if lon_d == 'W':
                lon = -lon
            message = {
                'data_id': data_id,
                'gps_date_time': gps_date_time,
                'lat': lat,
                # 'lat_d': values[2],
                'lon': lon,
                # 'lon_d': values[4],
                # 'utc_time': values[5],
                # 'warning': values[6],
                'mode': values[7],
            }
            result.append(message)

    def _parse_GPGSV(self, data_id, values, result):
        """
        GPGSVの解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        total_messages = int(values[1])
        message_number = int(values[2])
        # parse error in data_id:GPGSV values:['$GPGSV', '1', '1', '01', '18', '', '', '28']
        total_sv = int(values[3])
        for sv_in_message in range(4):
            sv_num = ((message_number - 1) * 4 + sv_in_message + 1)
            if sv_num > total_sv:
                break
            el_degree = values[5 + sv_in_message * 4]
            az_degree = values[6 + sv_in_message * 4]
            if len(az_degree)==0 :
                az_degree = 0
            if len(el_degree)==0:
                el_degree = 0

            message = {
                'data_id': data_id,
                # 'total_messages': total_messages,
                # 'message_number': message_number,
                'total_sv': total_sv,
                'sv_num': sv_num,
                # 'sv_in_message': sv_in_message,
                'sv_prn': values[4 + sv_in_message * 4],
                'el_degree': int(el_degree),
                'az_degree': int(az_degree),
            }
            srn = values[7 + sv_in_message * 4]
            if len(srn) > 0:
                message['srn'] = int(srn)
            else:
                message['srn'] = 0
            result.append(message)

    def _parse_GPGSA(self, data_id, values, result):
        """
        GPGSA電文の解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        if len(values[3]) == 0:
            logger.warning(f'no satellites in GPGSA')
        else:
            message = {
                'data_id': data_id,
                'mode': values[1],
                'type': values[2],
                'satellite_01': values[3],
                'satellite_02': values[4],
                'satellite_03': values[5],
                'satellite_04': values[6],
                'satellite_05': values[7],
                'satellite_06': values[8],
                'satellite_07': values[9],
                'satellite_08': values[10],
                'satellite_09': values[11],
                'satellite_10': values[12],
                'satellite_11': values[13],
                'satellite_12': values[14],
                'pdop': float(values[15]),
                'hdop': float(values[16]),
                'vdop': float(values[17]),
            }
            result.append(message)

    def _parse_GPVTG(self, data_id, values, result):
        if len(values[5]) == 0:
            logger.warning(f'no data in GPVTG')
        else:
            message = {
                'data_id': data_id,
                'course': values[1],
                # 'true_course': values[2],
                'm_course': values[3],
                # 'mag_course': values[4],
                # 'k_speed': values[5],
                # 'speed_k_unit': values[6],
                'speed': float(values[7]),
                # 'speed_m_unit': values[8],
                'mode': values[9],
            }
            result.append(message)

    def _parse_GPGGA(self, data_id, values, result):
        """
        GPGGAの解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        utc_date = datetime.utcnow().strftime('%d%m%y')
        utc_time = values[1]
        try:
            gps_date_time = datetime.strptime(utc_date + utc_time, '%d%m%y%H%M%S.%f').isoformat()
        except ValueError:
            logger.warning(f'date format error in GPRMC {values}')
            gps_date_time = datetime.now().isoformat()

        lat = values[2]
        if lat == '':
            logger.warning(f'GPGGA is not valid.')
        else:
            lat = self.parse_matrix_value(values[2])
            lon = self.parse_matrix_value(values[4])
            lat_d = values[3]
            lon_d = values[5]
            if lat_d == 'S':
                lat = -lat
            if lon_d == 'W':
                lon = -lon
            message = {
                'data_id': data_id,
                'gps_date_time': gps_date_time,
                'lat': lat,
                'lon': lon,
                'fix_quality': int(values[6]),
                'num_satellites': int(values[7]),
                'hdop': float(values[8]),
                'altitude': float(values[9]),
                # 'alt_m': values[10],
                'geoid_height': float(values[11]),
                # 'geo_m': values[12],
            }
            if len(values[13]) > 0:
                message['dgps_update'] = values[13]
                message['dgps_id'] = values[14]
            result.append(message)

    def _parse_GNRMC(self, data_id, values, result):
        """
        GNRMCの解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        warning = values[2]
        utc_time = values[1]
        utc_date = values[9]
        try:
            gps_date_time = datetime.strptime(utc_date + utc_time, '%d%m%y%H%M%S.%f').isoformat()
        except ValueError:
            logger.warning(f'date format error in GNRMC {values}')
            gps_date_time = datetime.now().isoformat()
        if warning == 'A':
            lat = self.parse_matrix_value(values[3])
            lon = self.parse_matrix_value(values[5])
            lat_d = values[4]
            lon_d = values[6]
            if lat_d == 'S':
                lat = -lat
            if lon_d == 'W':
                lon = -lon
            speed = float(values[7])
            message = {
                'data_id': data_id,
                'gps_date_time': gps_date_time,
                # 'warning': warning,
                'lat': lat,
                'lon': lon,
                'speed': speed,
                'mode': values[12],
            }
            if len(values[8]) > 0:
                message['course'] = float(values[8])
            if len(values[10]) > 0:
                variation = float(values[10])
                if values[11] == 'S':
                    variation = -variation
                message['variation'] = variation

            result.append(message)
        else:
            logger.warning(f'GNRMC status is in V. values:{values}')

    def _parse_GPRMC(self, data_id, values, result):
        """
        GPRMCの解析
        :param data_id:
        :param values:
        :param result:
        :return:
        """
        warning = values[2]
        utc_time = values[1]
        utc_date = values[9]
        try:
            gps_date_time = datetime.strptime(utc_date + utc_time, '%d%m%y%H%M%S.%f').isoformat()
        except ValueError:
            logger.warning(f'date format error in GPRMC {values}')
            gps_date_time = datetime.now().isoformat()
        if warning == 'A':
            lat = self.parse_matrix_value(values[3])
            lon = self.parse_matrix_value(values[5])
            lat_d = values[4]
            lon_d = values[6]
            if lat_d == 'S':
                lat = -lat
            if lon_d == 'W':
                lon = -lon
            speed = float(values[7])
            message = {
                'data_id': data_id,
                'gps_date_time': gps_date_time,
                # 'warning': warning,
                'lat': lat,
                'lon': lon,
                'speed': speed,
                'mode': values[12],
            }
            if len(values[8]) > 0:
                message['course'] = float(values[8])
            if len(values[10]) > 0:
                variation = float(values[10])
                if values[11] == 'S':
                    variation = -variation
                message['variation'] = variation

            result.append(message)
        else:
            logger.warning(f'GPRMC status is in V. values:{values}')
