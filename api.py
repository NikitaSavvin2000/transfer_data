import datetime
import requests
import io
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from setting import static_link, api
from influx import write_to_influxdb, get_last_timestamp

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
    date_start = '2023-01-01'
    date_start = datetime.strptime(date_start, '%Y-%m-%d')
    last_date = get_last_timestamp(id_sesor) # дергаем из таблицы последнюю дату
    last_date = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%SZ')
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

def write_data_sensor(id_sensor, name_sensor):
    df_sensor = general_period(id_sensor)
    write_to_influxdb(df_sensor, name_sensor)
    # записываем данные df_sensor в таблицу name_sensor


def write_data_all(dict_sensor):
    for name_sensor, id_sensor in dict_sensor:
        write_data_sensor(id_sensor, name_sensor)


def update_dict_sensor():
    dict_sensor = 0# чтение и препобразование в dict Csv
    return dict_sensor


id_sensor = ''
name_sensor = ''
write_data_sensor(id_sensor, name_sensor)