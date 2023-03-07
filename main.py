from api import write_data_all, update_dict_sensor

while True:
    choice = input("Введите 1 для функции 1, 2 для функции 2 или 3 для выхода: ")
    if choice == "1":
        write_data_all()
    elif choice == "2":
        update_dict_sensor()
    elif choice == "3":
        print("Выход из программы")
        break
    else:
        print("Некорректный ввод, попробуйте еще раз")
