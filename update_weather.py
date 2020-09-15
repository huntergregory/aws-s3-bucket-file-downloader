import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter

HOURLY_COLUMNS = [
    'HourlyDewPointTemperature',
    'HourlyDryBulbTemperature',
    'HourlyPrecipitation',
    # 'HourlySkyConditions',
    'HourlyStationPressure',
    'HourlyVisibility',
    'HourlyWindSpeed',
]
# 'HourlyPresentWeatherType' is replaced for each type later. This type is sometimes rarely recorded

# only main one not included is FZ
WEATHER_CODES = {
    'mist': 'BR', 
    'drizzle': 'DZ', 
    'fog': 'FG',
    'haze': 'HZ',
    'heavy_rain': '+RA',
    'rain': 'RA',
    'light_rain': '-RA',
    'snow': 'SN', 
    'thunder_storm': 'TS' # includes VCTS
}

WEATHER_COLUMNS = ['DATE'] + HOURLY_COLUMNS + list(WEATHER_CODES.keys())
START_DATE = '2018-02-01T00:00:00'
END_DATE = '2020-01-01T00:00:00'

def update_weather_df(filename, city):
    df = pd.read_csv(filename)
    df = df.sort_values('DATE')
    df = df[df['DATE'] >= START_DATE]
    df = df[df['DATE'] < END_DATE]
    df['aw_codes'] = df.HourlyPresentWeatherType.apply(lambda weather_type: str(weather_type).split('|')[0])
    df['aw_codes'] = df.aw_codes.apply(lambda codes: '' if codes == 'nan' else codes)
    for col_name, code in WEATHER_CODES.items():
        df[col_name] = df.aw_codes.apply(lambda codes_string: '' if len(codes_string) == 0 else 'Yes' if code in codes_string else 'No')
    df['rain'] = df.aw_codes.apply(lambda codes_string: '' if len(codes_string) == 0 else 'Yes' if 'RA' in codes_string and '+RA' not in codes_string and '-RA' not in codes_string else 'No')
    df = df[WEATHER_COLUMNS]
    df.to_csv('{}-updated-weather.csv'.format(city))
    for weather_type in WEATHER_CODES.keys():
        print('{}: {}'.format(weather_type, Counter(df[weather_type])))
    return df

# boston_weather = update_weather_df('./weather/boston-weather.csv', 'boston')
# nyc_weather = update_weather_df('./weather/nyc-weather-laguardia-airport.csv', 'nyc')
# columbus_weather = update_weather_df('./weather/columbus-weather-john-glen-airport.csv', 'columbus')
# portland_weather = update_weather_df('./weather/portland-weather-troutdale-airport.csv', 'portland')


# dc_weather = update_weather_df('./weather/dc-weather-washington-reagan-airport-arlington-va.csv')
# sf_weather = update_weather_df('./weather/sf-weather-downtown.csv', 'sf')
