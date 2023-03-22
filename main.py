from transfer_data.example import \
    read_all_sensors,\
    linear_devices, \
    sqlClient, \
    get_updated_df_sensors
from api import Read


print('If you want update sensors list Enter 1')
print('If you want add "read flag" to sensor for reading data Enter 2')
print('If you want upload fresh data from sensors Enter 3')
print('4 - Exit')

while True:
    print(100 * '-')
    choice = int(input("Enter you choice - "))
    if choice == 1:
        df_devices_all, created_table_name = read_all_sensors(linear_devices)
        df_sql = sqlClient.get_dataframe_from_sql_table(created_table_name)
        updated_df_sensors = get_updated_df_sensors(df_devices_all, df_sql, 'SensorId')
        sqlClient.write_to_table(created_table_name, updated_df_sensors)
    elif choice == 2:
        print(100*'-')
        print(f'Enter one or few SensorId values use , and whitespace. Then click buttom enter')
        print(f'Example: buscounter_1448468309, impulsesensor_1447936013')
        list_flag_sensors = input('Enter SensorId - ').split(", ")
        sqlClient.update_read_flag(list_flag_sensors)
        pass
    elif choice == 3:
        created_table_name = 'table_of_sensors'
        df_sql = sqlClient.get_dataframe_from_sql_table(created_table_name)
        sensor_id_list = df_sql[df_sql["Read_flag"] == "Y"]["SensorId"].tolist()
        for sensor_id in sensor_id_list:
            name_sensors = df_sql[df_sql["SensorId"] == sensor_id]["Name"].tolist()[0]
            name_sensor_format = Read.name_to_format(Read, name_sensors)
            print(name_sensor_format)
            last_date = sqlClient.select_last_date(name_sensor_format)
            print(last_date)
            name_sensor, df_general_period = Read.general_period(name_sensors, sensor_id, last_date)
    elif choice == 4:
        update_dict_sensor()
        break
    else:
        print("Некорректный ввод, попробуйте еще раз")
