from api import Read, Write, Sensors, Recive, InfluxDB

sensors = Sensors(r'Monitor_Data_Model-2.xlsx')
dict_sensors = sensors.read_sensors()

token_IDB = 'your token Influx'
org = "skip"
#bucket = "TEST_BUCKET"
url = "https://us-west-2-1.aws.cloud2.influxdata.com"

host = "192.168.1.72"
port = 8086

data = Read(
    token = "your token",
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
    for name_sensor, sensor_id in dict_sensors.items():
        name_sensor, df_general_period = data.general_period(name_sensor=name_sensor, sensor_id=sensor_id)
        client_WID.save_dataframe_to_influx(name_sensor, df_general_period)
result()