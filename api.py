from datetime import datetime, timedelta
from typing import re

from dateutil.relativedelta import relativedelta
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class Read:
    '''
    Read data sensors from https://data.smart1.eu
    Link on documentation API:
    https://data.smart1.eu/s/W3M4E8EkqMAPWqL?dir=undefined&path=%2F04%20SOFTWARE%20%26%20FIRMWARE%2F02%20PORTAL%2FEN&openfile=674
    input: token, static_link, read_interval
        where token - token from https://data.smart1.eu
            static_link - link for receiving data about sensor. About link in documentation API
            read_interval - link's parameters. About link's parameters in documentation API
    output: dict_data_sensor in format dict{key=Name : value=df_sensor}
        where Name - name sensor
            df_sensor - dataframe with columns=['Timestamp', 'Measurements']
    example: data = Read(
                token = <your token> ,
                static_link = 'https://portal.smart1.eu/export/data/csv/376/linear/',
                read_interval = 'month'
             )
            dict_data_sensor = data.general_period(name_sensor="PV Over-Production", sensor_id='arithmetic_1464947907')
            print(dict_data_sensor)
            > {'PV Over-Production':                  Timestamp Measurements
                0      2023-01-01 00:04:36            0
                1      2023-01-01 00:09:36            0
                ...
              }
    '''
    def __init__(self, token, static_link, read_interval):
        self.token = token
        self.static_link = static_link
        self.read_interval = read_interval
        pass

    def read_data_api(self, sensor_id, last_date):
        #last_date = datetime(2023, 1, 1)  #Нужно получить из записанных ранее данных
        str_date = last_date.strftime("%Y%m%d") + "/"
        # static_link = "https://portal.smart1.eu/export/data/csv/376/linear/month/detailed/"
        api_link = self.static_link + self.read_interval + '/detailed/' + str_date + sensor_id + self.token
        df = pd.read_csv(api_link, sep=';')
        df = df[['Timestamp', 'Value1']]
        df = df.rename(columns={'Value1': 'Measurements'})
        return df

    def general_period(self, name_sensor, sensor_id, last_date):
        '''
        date_start - date start of measurements.
        Real date_start = '2016-01-01', for example date_start = '2023-01-01'
        '''
        date_start = '2022-11-01'
        date_start = datetime.strptime(date_start, '%Y-%m-%d')
        # last_date = recive_last_date(table_name)
        '''
        last_date pull up from table influxdb with name as name_sensor
        last_date need for definition of the last upload date. 
        It need for definition necessary read interval
        '''
        if last_date is not None:
            last_date = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%SZ')
        else:
            last_date = date_start
        df_general_period = pd.DataFrame(columns=['Timestamp', 'Measurements'])
        yesterday_date = datetime.today() - timedelta(days=1)
        while yesterday_date > last_date:
            df_sensor = self.read_data_api(sensor_id = sensor_id, last_date=last_date) # Реалтзовать в функцию last_date
            df_general_period = pd.concat([df_general_period, df_sensor], ignore_index=True)
            dict_par = {'month': 'months', 'day': 'days', 'year': 'years'}
            last_date += relativedelta(**{dict_par[self.read_interval]: 1})
            df_general_period['Timestamp'] = pd.to_datetime(df_general_period['Timestamp'])
            #print(last_date, type(last_date))
        return name_sensor, df_general_period


class Write:
    '''
    Writing sensor data in table
    input: dict_data_sensor
        where dict_data_sensor is result function Read.general_period()
    output:
        Written sensor data in table
            where name table is name sensor
                sensor data is DataFrame with columns=['Timestamp', 'Measurements']
    example:
        write_data = Write()
        write_data.write_data_sensor(dict_data_sensor)
    '''
    def write_data_sensor(self, dict_data_sensor):
        name_sensor = list(dict_data_sensor.keys())[0]
        df_sensor = list(dict_data_sensor.values())[0]
        return name_sensor, df_sensor



class Recive:
    '''
    Recive last data from influsdb's table
    It need for definitions start date downloads
    input: table_name
        where table_name is name sensor
    output:
            last_date, where last_date is date in format YYYY-MM-DD HH:MM:SS <class 'datetime.datetime'>
    example: none
    '''
    def recive_last_date(self, table_name): # нужно написать функцию стягивающую последнее Timestamp из таблицы# table_name будет равно имени датчика
        pass


class Sensors:
    '''
    Recive data about sensors from exel table
    input: excel file with sensor, in excel file the required sensors marked colum "Flag" Y/N
    output: Dict sensors, where dict = {key = Name: value = SensorId}
    example: sensors = Sensors(r'Monitor_Data_Model-2.xlsx')
            dict_sensors = sensors.read_sensors()
            print(dict_sensors)
            > {'BESS DC Voltage': 'remotesensor_1433931027', 'State Of Charge': 'remotesensor_1433931057', ...}
    '''
    def __init__(self, file_exel):
        self.file_exel = file_exel

    def read_sensors(self):
        excel_file = pd.read_excel(self.file_exel, sheet_name=None)
        selected_dict = {}
        for sheet_name, sheet_data in excel_file.items():
            condition = sheet_data['Flag'] == 'Y'
            selected_data = sheet_data.loc[condition, ['SensorId', 'Name']]
            selected_data['Name'] = selected_data['Name'].apply(lambda x: x.strip('"'))
            selected_dict.update(dict(zip(selected_data['Name'], selected_data['SensorId'],)))

        return selected_dict


class InfluxDB:

    def __init__(self, token, org, url, host, port):
        self.token = token
        self.org = org
        self.url = url
        self.host = host
        self.port = port
        self.client = InfluxDBClient(url=f"http://{self.host}:{self.port}", token=self.token, org=self.org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def save_dataframe_to_influx(self, name_sensor, df_sensor):
        buckets = self.client.buckets_api().find_buckets()
        buckets = buckets.to_dict()
        list_buckets = [bucket['name'] for bucket in buckets['buckets']]
        print(list_buckets)
        bucket_exists = any(bucket == name_sensor for bucket in list_buckets)
        org_id = self.client.organizations_api().find_organizations()[0].id
        if not bucket_exists:
            self.client.buckets_api().create_bucket(
                bucket_name=name_sensor,
                org_id=org_id
            )
        df_sensor["Timestamp"] = pd.to_datetime(df_sensor["Timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        df_sensor["Measurements"] = pd.to_numeric(df_sensor["Measurements"])
        points = []
        for _, row in df_sensor.iterrows():
            point = Point("Measurements").field(
                "value", row['Measurements']).time(row['Timestamp'],
                                                   WritePrecision.NS
                                                   )
            points.append(point)
        self.write_api.write(
            bucket=name_sensor,
            org=org_id,
            record=points
        )


'''
Full example using code you can view in 'example' directory -> example.py
'''
