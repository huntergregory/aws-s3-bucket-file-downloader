# FIXME why is difference always positive?
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm

from column_conversions import START_DATETIME, FINAL_COLUMNS
from update_weather import WEATHER_COLUMNS, WEATHER_CODES

WEATHER_COLUMNS = WEATHER_COLUMNS[1:] # omit 'DATE'
TIME_SINCE_COLUMNS = ['time_since_{}'.format(weather_column) for weather_column in WEATHER_COLUMNS]
weather_types = list(WEATHER_CODES.keys())
TIME_SINCE_WEATHER_TYPE = 'time_since_weather_type'

def references_weather_type(col_name):
    return any([weather_type in col_name for weather_type in weather_types])

RESULTING_COLUMNS = FINAL_COLUMNS + WEATHER_COLUMNS + [col for col in TIME_SINCE_COLUMNS if not references_weather_type(col)] + [TIME_SINCE_WEATHER_TYPE]

FOLDER = 'final-bss-data'
def add_nearest_weather(city):
    print('starting bss csv download')
    bss = pd.read_csv('../{}/{}_bss.csv'.format(FOLDER, city))
    print('finished downloading bss csv')
    bss = bss.sort_values(START_DATETIME)
    weather = pd.read_csv('{}-updated-weather.csv'.format(city))
    weather = weather.sort_values('DATE')
    weather_na = weather.isna()

    weather_at_start = None
    weather_indices = [0] * len(WEATHER_COLUMNS)
    print('starting loop')
    for bss_index in tqdm(range(0, len(bss))):
        start_datetime = datetime.strptime(bss[START_DATETIME].iloc[bss_index], '%Y-%m-%d %H:%M:%S')
        differences = []
        for k in range(0, len(weather_indices)):
            weather_index = weather_indices[k]
            if weather_index == len(weather) - 1:
                continue
            difference = np.Infinity # in minutes
            while weather_index < len(weather):
                if not weather_na[WEATHER_COLUMNS[k]].iloc[weather_index]:
                    weather_datetime = datetime.strptime(weather.DATE.iloc[weather_index], '%Y-%m-%dT%H:%M:%S')
                    delta = weather_datetime - start_datetime
                    new_difference = delta.days * 86400 + delta.seconds
                    if abs(new_difference) > abs(difference):
                        break
                    difference = new_difference
                weather_index += 1
            if weather_index == len(weather):
                weather_index = len(weather) - 1
                print('[WARN] weather index for {} reached last row at bss_index {}'.format(WEATHER_COLUMNS[k], bss_index))
                print('start date: {} \t final weather date: {}'.format(start_datetime, datetime.strptime(weather.DATE.iloc[weather_index], '%Y-%m-%dT%H:%M:%S')))
                print('difference: {}'.format(difference))
            weather_indices[k] = weather_index
            differences.append(difference)
        weather_values = {col_name: weather[col_name].iloc[i] for col_name, i in zip(WEATHER_COLUMNS, weather_indices)}
        for col_name, diff in zip(TIME_SINCE_COLUMNS, differences):
            if references_weather_type(col_name):
                weather_values[TIME_SINCE_WEATHER_TYPE] = round(diff, 2)
            else: 
                weather_values[col_name] = round(diff, 2)
        df_row = pd.DataFrame(weather_values, index=[bss_index])
        if weather_at_start is None:
            weather_at_start = df_row
        else:
            weather_at_start = weather_at_start.append(df_row)
    result = pd.concat([bss, weather_at_start], axis=1, sort=False)
    result = result[RESULTING_COLUMNS]
    result.to_csv('complete_{}_bss.csv'.format(city))

# add_nearest_weather('columbus')
add_nearest_weather('portland')
# add_nearest_weather('boston')


# columbus_bss = pd.read_csv('../final-bss-data/columbus_bss.csv')
# columbus_bss.start_datetime.iloc[5175]
# weather = pd.read_csv('columbus-updated-weather.csv')

# last_weather_date = datetime.strptime(weather.DATE.iloc[len(weather) - 1], '%Y-%m-%dT%H:%M:%S')
# columbus_date = datetime.strptime(columbus_bss.start_datetime.iloc[5175], '%Y-%m-%d %H:%M:%S')