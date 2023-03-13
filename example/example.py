from datetime import datetime
import re

from api import Read, Write, Sensors, Recive, InfluxDB
from sql import SQLWrite

"""
Data for SQL client bellow
"""
sql_db_host = 'localhost'
sql_db_port = 5432
sql_db_name = 'sensors2'
sql_db_user = 'postgres'
sql_db_password = 'Welcome123'



sensors = Sensors(r'Monitor_Data_Model-2.xlsx')
dict_sensors = sensors.read_sensors()

data = Read(
    token = "?apikey=6baa1316e5a78fbde7cec5735834245f"
,
    static_link = 'https://portal.smart1.eu/export/data/csv/376/linear/',
    read_interval = 'month'
)

write_data = Write()

token_IDB = '3NG4cYNKJG3Z92kdpWu0qgZL-EecLMT76U4LUWAzQy4n22-ARjY2sG9BoJje_ltECmVaIzAjPuPzF2bLJ1IOfw=='
org = "skip"
#bucket = "TEST_BUCKET"
url = "https://us-west-2-1.aws.cloud2.influxdata.com"

host = "192.168.1.72"
port = 8086

client_WID = InfluxDB(token=token_IDB, org=org, url=url, host=host, port=port)

'''
In example directory will be created 'Test result'.
Name folder you can change in influx.py function - save_dataframe_to_csv, variable - name_folder
'''

def result():
    sqlClient = SQLWrite(
        db_host=sql_db_host,
        db_port=sql_db_port,
        db_name=sql_db_name,
        db_user=sql_db_user,
        db_password=sql_db_password
    )
    sqlClient.create_connection()
    for name_sensor, sensor_id in dict_sensors.items():
        name_sensor = ''.join(['_' if not c.isalnum() else c for c in name_sensor])
        name_sensor = name_sensor.rstrip('_')
        name_sensor = '_'.join([w for w in name_sensor.split('_') if w])
        last_date = sqlClient.select_last_date(name_sensor)
        print(100 * '-')
        print(f'Receiving sensor data "{name_sensor}" from site https://data.smart1.eu')
        name_sensor, df_general_period = data.general_period(
            name_sensor=name_sensor, sensor_id=sensor_id, last_date=last_date
        )
        #client_WID.save_dataframe_to_influx(name_sensor, df_general_period)
        print(f'Uploading sensor data "{name_sensor}" in PostgreSQL')
        sqlClient.write_to_postgresql(name_sensor, df_general_period)
    sqlClient.saving_data()
    sqlClient.kill_connection()

result()

'''sqlClient = SQLWrite(
    db_host=sql_db_host,
    db_port=sql_db_port,
    db_name=sql_db_name,
    db_user=sql_db_user,
    db_password=sql_db_password
)

sqlClient.create_connection()

sqlClient.table_list()
last_date = sqlClient.select_last_date('solar_irradiance_s')


print(last_date)'''
