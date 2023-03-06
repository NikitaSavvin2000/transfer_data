from datetime import datetime as dt, timedelta as td
import pandas as pd


def dates():
    date_test = "03.05.2022" # Pull up last date from general table
    last_value_date = dt.strptime(date_test, "%d.%m.%Y")
    date_now = dt.now()
    date_today = dt(year=date_now.year, month=date_now.month, day=date_now.day)
    date_yesterday = date_today - td(days=1) # Using yesyerday date because all data
                                                # there is only for that day which ended
    return last_value_date, date_yesterday


def create_date_df(last_value_date, date_yesterday):
    date_list = pd.date_range(last_value_date, date_yesterday, freq='5min').tolist()
    df = pd.DataFrame(date_list, columns=['datetime'])
    return df

last_value_date, date_yesterday = dates()
df = create_date_df(last_value_date, date_yesterday)
