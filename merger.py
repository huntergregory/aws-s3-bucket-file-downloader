import pandas as pd
import os
from tqdm import tqdm
from column_conversions import FINAL_COLUMNS, CITIES, convert_df 

# boston_weather = pd.read_csv('./weather/boston-weather.csv')
# dc_weather = pd.read_csv('./weather/dc-weather-washington-reagan-airport-arlington-va.csv')
# sf_weather = pd.read_csv('./weather/sf-weather-downtown.csv')
# nyc_weather = pd.read_csv('./weather/nyc-weather-laguardia-airport.csv')
# columbus_weather = pd.read_csv('./weather/columbus-weather-john-glen-airport.csv')
# portland_weather = pd.read_csv('./weather/portland-weather-troutdale-airport.csv')

all_bss = None
for city in CITIES:
    folder = './bss/{}/'.format(city)
    print('Starting to merge files for {}'.format(city))
    city_bss = None
    with os.scandir(folder) as files:
        for file in tqdm(sorted(files, key=lambda file: file.name)):
            if file.name.endswith('.csv') and file.is_file():
                df = pd.read_csv(file.path)
                print('Processing {}'.format(file.name))
                converted_df = convert_df(df, city)
                # add_weather(converted_df, city)
                if city_bss is None:
                    city_bss = converted_df
                else:
                    city_bss = city_bss.append(converted_df)
    city_bss = city_bss[FINAL_COLUMNS]
    city_bss.to_csv('{}_bss.csv'.format(city))
# FIXME uncomment rest if you have enough space on disk to merge all csvs at once
#     if all_bss is None:
#         all_bss = city_bss
#     else:
#         all_bss.append(city_bss)

# all_bss = all_bss[FINAL_COLUMNS]
# all_bss.to_csv('all_bss.csv')


## Useful for EDA when station names and coordinates were included. Checking coordinates for stations
# start_cols = ['start_station_name', 'start_latitude', 'start_longitude']
# end_cols = ['end_station_name', 'end_latitude', 'end_longitude']

# def get_station_df(df, cols):
#     renamer = {cols[0]: 'station_name', cols[1]: 'latitude', cols[2]: 'longitude'}
#     result = df[cols].rename(columns=renamer)
#     result[['latitude', 'longitude']] = result[['latitude', 'longitude']].apply(lambda x: round(x, 6))
#     return result

# stations = get_station_df(full_columbus, start_cols).append(get_station_df(full_columbus, end_cols)).drop_duplicates().sort_values('station_name')
# len(stations.station_name.unique())
# len(stations)
# stations.latitude.iloc[0] == stations.latitude.iloc[1]