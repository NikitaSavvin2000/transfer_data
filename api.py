import datetime
import requests
import io
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from setting import static_link, api

static_link = static_link
api = api

def read_data_api(id_sesor, last_date):
    #last_date = datetime.date(2022, 3, 14)
    str_date = last_date.strftime("%Y%m%d") + "/"
    global static_link
    global api
    #static_link = "https://portal.smart1.eu/export/data/csv/376/linear/month/detailed/"
    #api = "your api"
    api_link = static_link + str_date + id_sesor + api
    r = requests.get(api_link)
    # Convert the response to a pandas DataFrame
    df = pd.read_csv(io.StringIO(r.text), sep =';')
    # Drop the original column
    # Show the resulting DataFrame
    df = df[['Timestamp', 'Value1']]
    return df


def general_period(id_sesor):
    date_start = '2023-01-01' # Здесь делаем SQL запрос в базу данных по конкретному датчику
    date_start = datetime.strptime(date_start, '%Y-%m-%d')
    last_date = None
    if last_date is None:
        last_date = date_start
    df_general_period = pd.DataFrame(columns=['Timestamp', 'Value1'])
    yesterday_date = datetime.today() - timedelta(days=1)
    while yesterday_date > last_date:
        df_sensor = read_data_api(id_sesor, last_date) # Реалтзовать в функцию last_date
        df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)
        last_date += relativedelta(months=1)
    return df_general_period


id_sesor = 'your id senser'

# Импортируем необходимые библиотеки
'''
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
            'measurement': table_name,  # Указываем имя таблицы
            'tags': {  # Определяем теги для точки данных
                'tag1': row['tag1'],
                'tag2': row['tag2']
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


test = write_dataframe_to_influxdb(one, 'test')
print(test)

'''
def write_data_sensor(id_sensor, name_sensor):
    list_table_name = '' # запрос в базу даных о всех таблицах
    if name_sensor is not list_table_name:
        # создаем таблицу
    df_sensor = general_period(id_sensor)
    # записываем данные df_sensor в таблицу name_sensor
    pass


def write_data_all(dict_sensor):
    for id_sensor, name_sensor in dict_sensor:
        write_data_sensor(id_sensor, name_sensor)


def update_dict_sensor():
    dict_sensor = 0# чтение и препобразование в dict Csv
    return dict_sensor
