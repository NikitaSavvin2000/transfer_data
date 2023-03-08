import pandas as pd
import datetime
import requests
import io

def read_data_api(id_sesor, name_sensor):
    last_date = datetime.date(2022, 3, 14)
    str_date = last_date.strftime("%Y%m%d") + "/"
    sensor_name = "Grid Feed"
    static_link = "https://portal.smart1.eu/export/data/csv/376/linear/month/detailed/"
    api = "?apikey=6baa1316e5a78fbde7cec5735834245f"
    api_link = static_link + str_date + id_sesor + api 
    r = requests.get(api_link)
    # Convert the response to a pandas DataFrame
    df = pd.read_csv(io.StringIO(r.text))
    df = df.loc[:, ['Timestamp', 'Value1']]
    return df

one = read_data_api("remotesensor_1433931027", "Grid Feed")
print(one)


# Импортируем необходимые библиотеки
from influxdb import InfluxDBClient
import pandas as pd

# Определяем функцию для записи DataFrame в InfluxDB
def write_dataframe_to_influxdb(df, table_name):
    # Подключаемся к InfluxDB
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('mydb')

    # Преобразуем DataFrame в формат InfluxDB
    data = []
    for index, row in df.iterrows():
        # Формируем точку данных для каждой строки DataFrame
        point = {
            'measurement': table_name, # Указываем имя таблицы
            'tags': { # Определяем теги для точки данных
                'tag1': row['tag1'],
                'tag2': row['tag2']
            },
            'time': row['timestamp'].strftime('%Y-%m-%dT%H:%M:%SZ'), # Указываем временную метку в формате UTC
            'fields': { # Определяем поля для точки данных
                'field1': row['field1'],
                'field2': row['field2']
            }
        }
        data.append(point) # Добавляем точку данных в список

    # Записываем данные в InfluxDB
    #client.write_points(data)
    return data

test = write_dataframe_to_influxdb(<df>, <table_name>)
print(test)