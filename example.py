from datetime import datetime
import re

from api import Read, Write, Sensors, Recive, InfluxDB
from sql import SQLWrite, dict_sql_types
import pandas as pd


"""
Data for SQL client bellow
"""
sql_db_host = 'localhost'
sql_db_port = 5432
sql_db_name = 'sensors2'
sql_db_user = 'postgres'
sql_db_password = '1234'



#sensors = Sensors(r"/Users/dmitrii/Desktop/PhD/Python/Script_DB/transfer_data/Monitor_Data_Model-2.xlsx")
#dict_sensors = sensors.read_sensors()

data = Read(
    token = "?apikey=6baa1316e5a78fbde7cec5735834245f"
,
    static_link = 'https://portal.smart1.eu/export/data/csv/376/linear/',
    read_interval = 'day'
)

write_data = Write()

token_IDB = '3NG4cYNKJG3Z92kdpWu0qgZL-EecLMT76U4LUWAzQy4n22-ARjY2sG9BoJje_ltECmVaIzAjPuPzF2bLJ1IOfw=='
org = "skip"
#bucket = "TEST_BUCKET"
url = "https://us-west-2-1.aws.cloud2.influxdata.com"


'''
In example directory will be created 'Test result'.
Name folder you can change in influx.py function - save_dataframe_to_csv, variable - name_folder
'''

'''def result():
    sqlClient = SQLWrite(
        db_host=sql_db_host,
        db_port=sql_db_port,
        db_name=sql_db_name,
        db_user=sql_db_user,
        db_password=sql_db_password
    )
    sqlClient.create_connection()
    for name_sensor, sensor_id in dict_sensors.items():      
        last_date = sqlClient.select_last_date(name_sensor)
        print(last_date, type(last_date))
        print(100 * '-')
        print(f'Receiving sensor data "{name_sensor}" from site https://data.smart1.eu')
        name_sensor, df_general_period = data.general_period(
            name_sensor=name_sensor, sensor_id=sensor_id, last_date=last_date
        )
        print(f'Uploading sensor data "{name_sensor}" in PostgreSQL')
        sqlClient.write_to_postgresql(name_sensor, df_general_period)
    sqlClient.saving_data()
    sqlClient.kill_connection()'''

#result()

linear_devices = ['counters', 'sensors', 'regulations']

def read_all_sensors(linear_devices):
    table_name = 'Table_of_sensors'
    columns = ['SensorId', 'Name', 'Type']
    df_devices_all = pd.DataFrame(columns=columns)
    dfs = []
    for device_type in linear_devices:
        data_devices = Read(
            token="?apikey=6baa1316e5a78fbde7cec5735834245f", 
            static_link=f"https://portal.smart1.eu/export/{device_type}/376",
            read_interval="none"
        )
        df_devices = data_devices.read_sensors()
        if device_type == "counters":
            df_devices.rename(columns={'Counter Id': 'SensorId'}, inplace=True)
        df_devices = df_devices[columns]
        dfs.append(df_devices)
    df_devices_all = pd.concat(dfs, ignore_index=True)
    return df_devices_all, table_name

def result():
    sqlClient = SQLWrite(
        db_host=sql_db_host,
        db_port=sql_db_port,
        db_name=sql_db_name,
        db_user=sql_db_user,
        db_password=sql_db_password
    )
    sqlClient.create_connection()
    df_devices_all, created_table_name = read_all_sensors(linear_devices)
    sqlClient.create_table(created_table_name)
    table_names_list = sqlClient.table_list()
    print(created_table_name)
    sqlClient.add_columns_to_existing_table(df_devices_all, created_table_name)
    """for col_name in df_devices_all.columns.tolist():
        print(col_name)
        type_column = str(df_devices_all[col_name].dtype)
        sqlClient.create_column(created_table_name, col_name, type_column)"""
    
    # Print all created columns in created_table_name
    """created_columns = sqlClient.show_col_name(created_table_name)
    for column in created_columns:
        print(column.name)
   
    sqlClient.write_to_table(created_table_name, df_devices_all)
    sqlClient.saving_data()
    sqlClient.kill_connection()"""


result()

#example = read_all_sensors(linear_devices)
#print(example.dtypes)
'''name_sensor = "Grid Feed"
sensor_id = 'buscounter_1433927661'
last_date = None
name_sensor, df_general_period_1 = data.general_period(
            name_sensor=name_sensor, sensor_id=sensor_id, last_date=last_date    
                                )       
x = str(df_general_period_1['grid_feed'].dtype)
print(x)
print(dict_sql_types[x])'''

