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
cursor.execute("CREATE TABLE IF NOT EXISTS GPRMC(id integer,name text)")

connection.close()