from datetime import datetime as dt, timedelta as td
import pandas as pd


def dates():
    last_date_table = "05.03.2022" # Pull up last date from general table
    #last_date_table = None # For test if table empty
    if last_date_table is None:
        last_date_table = "04.03.2023" # When run code first time for fresh, empty table, last_date_table = = 01.01.2016
    last_value_date = dt.strptime(last_date_table, "%d.%m.%Y")
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
print(df)

