from api import Read, Write, Sensors, Recive


sensors = Sensors(r'Monitor_Data_Model-2.xlsx')
dict_sensors = sensors.read_sensors()

data = Read(
    token = '?apikey=6baa1316e5a78fbde7cec5735834245f',
    static_link = 'https://portal.smart1.eu/export/data/csv/376/linear/',
    read_interval = 'month'
)

write_data = Write()

name_folder = 'result'

'''
In example directory will be created 'Test result'.
Name folder you can change in influx.py function - save_dataframe_to_csv, variable - name_folder
'''

def result():
    for name_sensor, sensor_id in dict_sensors.items():
        dict_data_sensor = data.general_period(name_sensor=name_sensor, sensor_id=sensor_id)
        write_data.write_data_sensor(dict_data_sensor)

result()