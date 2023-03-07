import pickle
from api import update_dict_sensor


dict_sensor = update_dict_sensor()
with open('my_var.pickle', 'wb') as f:
    pickle.dump(dict_sensor, f)
