import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import sqlite3

JST = timezone(timedelta(hours=+9), 'JST')

SQLITE_DB_NAME = os.environ.get('SQLITE_DB_NAME', default='data/gnss_%Y%m%d.db')
db_name = datetime.now(JST).strftime(SQLITE_DB_NAME)
connection = sqlite3.connect(db_name)
cursor = connection.cursor()

# GPRMC,GPGGA,GPVTG,GPGSA,GPGSV,GPGLL,GPTXT
# 共通：　device_id, local_time, data_id,
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS GPRMC(
        device_id TEXT, 
        local_time TEXT, 
        data_id TEXT, 
        gps_date_time TEXT, 
        lat REAL, 
        lon REAL, 
        speed REAL, 
        mode TEXT
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS GPGGA(
        device_id TEXT, 
        local_time TEXT, 
        data_id TEXT, 
        gps_date_time TEXT, 
        lat REAL, 
        lon REAL, 
        fix_quality　INTEGER,
        num_satellites INTEGER,
        hdop REAL, 
        altitude REAL,
        geoid_height REAL
    )
    """
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS GPVTG(
        device_id TEXT, 
        local_time TEXT, 
        data_id TEXT, 
        m_course TEXT,
        speed REAL,
        mode TEXT
    )
    """
)

connection.close()