from datetime import datetime
from influxdb import InfluxDBClient
import pickle

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
token = "u2z-6PsPSDv002ENSRlUZtxTWl49BWSkD49J8FU0Klx3UcmgB8N5GX8mFPgaWdS3hNqgjNzu75m6K95oWEneqQ=="
org = "University"
bucket = "test_db"

with open('dict_sensor.pickle', 'rb') as f:
    dict_sensor = pickle.load(f)


def get_last_timestamp(id_sesor):
    table_name = dict_sensor.values(id_sesor)
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('test_db')
    query = f'SELECT "Timestamp" FROM {table_name} ORDER BY time DESC LIMIT 1'
    result = client.query(query)
    last_date = list(result.get_points())[0]['Timestamp']
    last_date = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%SZ')
    return last_date

def write_to_influxdb(df, table_name):
    # Соединяемся с базой данных InfluxDB
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('test_db')
    # Преобразуем DataFrame в формат InfluxDB
    data = []
    for index, row in df.iterrows():
        # Формируем точку данных для каждой строки DataFrame
        point = {
            'measurement': table_name,  # Указываем имя таблицы
            'tags': {  # Определяем теги для точки данных
                'tag1': row['Timestamp'],
                'tag2': row['Value1']
            },
            'time': row['timestamp'].strftime('%Y-%m-%dT%H:%M:%SZ'),  # Указываем временную метку в формате UTC
            'fields': {  # Определяем поля для точки данных
                'field1': row['field1'],
                'field2': row['field2']
            }
        }
        data.append(point)  # Добавляем точку данных в список

    # Записываем данные в InfluxDB
    # client.write_points(data)
    return data