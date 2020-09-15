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

def add_nearest_weather(city, debug):
    print('starting bss csv download')
    bss = pd.read_csv('../final-bss-data/{}_bss.csv'.format(city))
    print('finished downloading bss csv')
    bss = bss.sort_values(START_DATETIME)
    weather = pd.read_csv('{}-updated-weather.csv'.format(city))
    weather = weather.sort_values('DATE')
    weather_na = weather.isna()

    weather_at_start = None
    weather_indices = [[i for i, is_na in enumerate(weather_na[col_name]) if not is_na][0] for col_name in WEATHER_COLUMNS]
    print('starting loop')
    end = len(bss) if not debug else 100
    for bss_index in tqdm(range(0, end)):
        start_datetime = datetime.strptime(bss[START_DATETIME].iloc[bss_index], '%Y-%m-%d %H:%M:%S')

        def get_diff(index):
            weather_datetime = datetime.strptime(weather.DATE.iloc[index], '%Y-%m-%dT%H:%M:%S')
            delta = weather_datetime - start_datetime
            return (delta.days * 24 + delta.seconds / 3600) # hours

        differences = []
        for k in range(0, len(weather_indices)):
            difference = get_diff(weather_indices[k]) # in minutes
            while weather_indices[k] < len(weather) - 1:
                new_index = weather_indices[k] + 1
                while new_index < len(weather) - 1 and weather_na[WEATHER_COLUMNS[k]].iloc[new_index]:
                    new_index += 1
                new_difference = get_diff(new_index)
                if abs(new_difference) > abs(difference):
                    break
                difference = new_difference
                weather_indices[k] = new_index
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
    bss_for_concat = bss if not debug else bss.iloc[0:100]
    result = pd.concat([bss_for_concat, weather_at_start], axis=1, sort=False)
    result = result[RESULTING_COLUMNS]
    if debug:
        return result
    else:
        result.to_csv('complete_{}_bss.csv'.format(city))

# add_nearest_weather('columbus', False)
# add_nearest_weather('portland', False)
# add_nearest_weather('boston', False)
# add_nearest_weather('nyc', True)

# portland_bss = pd.read_csv('../final-bss-data/portland_bss.csv')
# boston_bss = pd.read_csv('../final-bss-data/boston_bss.csv')
# columbus_bss = pd.read_csv('../final-bss-data/columbus_bss.csv')
# columbus_bss.start_datetime.iloc[5175]
# weather = pd.read_csv('columbus-updated-weather.csv')
# good_weather = weather[weather.snow.notna()]
# good_weather.DATE.apply(lambda x: x[:10]).unique()

# weather_date = datetime.strptime(weather.DATE.iloc[1162], '%Y-%m-%dT%H:%M:%S')
# columbus_date = datetime.strptime(columbus_bss.start_datetime.iloc[1000], '%Y-%m-%d %H:%M:%S')
# len(weather.DATE[weather.snow.notna()].unique()) == sum(weather.snow.notna())
# delta = last_weather_date - columbus_date
# return (delta.days * 24 + delta.seconds / 3600) # hours
