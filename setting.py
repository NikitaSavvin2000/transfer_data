import pickle
from api import update_dict_sensor
import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

SECRET_KEY = os.environ.get("SECRET_KEY")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")
static_link = "https://portal.smart1.eu/export/data/csv/376/linear/month/detailed/"
api = "?apikey=6baa1316e5a78fbde7cec5735834245f"

dict_sensor = update_dict_sensor()
with open('dict_sensor.pickle', 'rb') as f:
    pickle.dump(dict_sensor, f)
