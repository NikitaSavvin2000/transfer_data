from api import write_data_all, update_dict_sensor
import pickle

with open('dict_sensor.pickle', 'rb') as f:
    dict_sensor = pickle.load(f)

while True:
    choice = input("Введите 1 для функции 1, 2 для функции 2 или 3 для выхода: ")
    if choice == "1":
        write_data_all(dict_sensor)
    elif choice == "2":
        update_dict_sensor()
    elif choice == "3":
        print("Выход из программы")
        break
    else:
        print("Некорректный ввод, попробуйте еще раз")
