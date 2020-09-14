from datetime import datetime

TRIP_DURATION = 'trip_duration' # computed for all from START_DATETIME and STOP_TIME. Don't use duration precomputed by some
START_DATETIME = 'start_datetime'
END_DATETIME = 'end_datetime'
START_DAY_OF_WEEK = 'start_day_of_week' # computed from START_DATETIME
START_LAT = 'start_latitude'
START_LONG = 'start_longitude'
END_LAT = 'end_latitude'
END_LONG = 'end_longitude'
START_STATION_NAME = 'start_station_name'
END_STATION_NAME = 'end_station_name'
BIRTH_YEAR = 'birth_year' # some will be NA
GENDER = 'gender' # some will be NA
IS_SUBSCRIBER = 'is_subscriber' # categories need to be unified
CITY = 'city'

FINAL_COLUMNS = [TRIP_DURATION, START_DATETIME, END_DATETIME, START_DAY_OF_WEEK, START_LAT, START_LONG, END_LAT, END_LONG, START_STATION_NAME, END_STATION_NAME, BIRTH_YEAR, GENDER, IS_SUBSCRIBER, CITY]

class Converter():
    def __init__(self, city_name, conversions, no_info_list=[]):
        super().__init__()
        self.city_name = city_name
        self.conversions = conversions
        self.no_info_list = no_info_list

    def convert(self, df):
        info_list = set([START_STATION_NAME, END_STATION_NAME, GENDER, BIRTH_YEAR]) - set(self.no_info_list)
        self.update_pre_conversion(df)
        specific_conversions = {original: replacement for original, replacement in self.conversions.items() if original in df.columns}

        df.rename(columns=specific_conversions, inplace=True)

        info_list -= set(df.columns)
        if len(info_list) > 0:
            print("[INFO] the following columns weren't in the df before final updates: {}".format(info_list))
        
        subscriber_values = sorted(df[IS_SUBSCRIBER].unique())
        unique_options = [['Customer', 'Subscriber'], ['Casual', 'Subscriber'], ['Customer', 'Dependent', 'Subscriber']]
        if any([subscriber_values == options for options in unique_options]):
            df[IS_SUBSCRIBER] = df[IS_SUBSCRIBER].apply(lambda item: 'Yes' if item == 'Subscriber' else 'No')
        elif  subscriber_values == ['casual', 'member']:
            df[IS_SUBSCRIBER] = df[IS_SUBSCRIBER].apply(lambda item: 'Yes' if item == 'member' else 'No')
        else:
            print("[WARN] subscriber categories weren't set correctly")
        
        self.add_computed_values(df)
        self.update_post_conversion(df)
        for column in FINAL_COLUMNS:
            if column not in df.columns:
                df[column] = ''
        return df[FINAL_COLUMNS]

    def add_computed_values(self, df):
        df[CITY] = [self.city_name] * len(df)
        df[START_DATETIME] = df[START_DATETIME].apply(lambda date_string: date_string[:19]) # removes decimals. Date format must be 2019-01-31 17:57:44.1234
        df[END_DATETIME] = df[END_DATETIME].apply(lambda date_string: date_string[:19]) # removes decimals. Date format must be 2019-01-31 17:57:44.1234

        def get_datetime(date_string):
            return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        start_datetimes = df[START_DATETIME].apply(get_datetime)
        end_datetimes = df[END_DATETIME].apply(get_datetime)
        df[START_DAY_OF_WEEK] = start_datetimes.apply(lambda date: date.strftime('%A'))
        df[TRIP_DURATION] = start_datetimes - end_datetimes
        df[TRIP_DURATION] = df[TRIP_DURATION].apply(lambda time_delta: time_delta.total_seconds())
    
    def update_pre_conversion(self, df):
        pass # TODO implement in subclass
    
    def update_post_conversion(self, df):
        pass # TODO implement in subclass


## NYC
# Since February 2018: ['tripduration', 'starttime', 'stoptime', 'start station id', 'start station name', 'start station latitude', 'start station longitude', 'end station id', 'end station name', 'end station latitude', 'end station longitude', 'bikeid', 'usertype', 'birth year', 'gender']
NYC_CONVERSIONS = {}
NYC_CONVERSIONS['starttime'] = START_DATETIME
NYC_CONVERSIONS['stoptime'] = END_DATETIME
NYC_CONVERSIONS['start station latitude'] = START_LAT
NYC_CONVERSIONS['start station longitude'] = START_LONG
NYC_CONVERSIONS['end station latitude'] = END_LAT
NYC_CONVERSIONS['end station longitude'] = END_LONG
NYC_CONVERSIONS['start station name'] = START_STATION_NAME
NYC_CONVERSIONS['end station name'] = END_STATION_NAME
NYC_CONVERSIONS['birth year'] = BIRTH_YEAR
NYC_CONVERSIONS['gender'] = GENDER
NYC_CONVERSIONS['usertype'] = IS_SUBSCRIBER

class NycConverter(Converter):
    def __init__(self):
        super().__init__('nyc', NYC_CONVERSIONS)

## Portland
# From July 2016: ['BikeID', 'BikeName', 'Distance_Miles', 'Duration', 'EndDate', 'EndHub', 'EndLatitude', 'EndLongitude', 'EndTime', 'MultipleRental', 'PaymentPlan', 'RentalAccessPath', 'RouteID', 'StartDate', 'StartHub', 'StartLatitude', 'StartLongitude', 'StartTime', 'TripType']
# Inconsistency starting June 2016:
# ['Distance_Miles_', 'End_Latitude', 'End_Longitude', 'Start_Latitude', 'Start_Longitude']
# vs.
# ['Distance_Miles', 'EndLatitude', 'EndLongitude', 'StartLatitude', 'StartLongitude']
PORTLAND_CONVERSIONS = {}
# PORTLAND_CONVERSIONS['StartTime'] = SIMPLE_START_TIME
PORTLAND_CONVERSIONS['StartLatitude'] = START_LAT
PORTLAND_CONVERSIONS['Start_Latitude'] = START_LAT
PORTLAND_CONVERSIONS['StartLongitude'] = START_LONG
PORTLAND_CONVERSIONS['Start_Longitude'] = START_LONG
PORTLAND_CONVERSIONS['EndLatitude'] = END_LAT
PORTLAND_CONVERSIONS['End_Latitude'] = END_LAT
PORTLAND_CONVERSIONS['EndLongitude'] = END_LONG
PORTLAND_CONVERSIONS['End_Longitude'] = END_LONG
PORTLAND_CONVERSIONS['StartHub'] = START_STATION_NAME
PORTLAND_CONVERSIONS['EndHub'] = END_STATION_NAME
PORTLAND_CONVERSIONS['PaymentPlan'] = IS_SUBSCRIBER

def reformat_date(date_string):
    month, day, year = date_string.split('/')
    if len(month) == 1:
        month = '0' + month
    if len(day) == 1:
        day = '0' + day
    return '{}-{}-{}'.format(year, month, day)

class PortlandConverter(Converter):
    def __init__(self):
        super().__init__('portland', PORTLAND_CONVERSIONS, [GENDER, BIRTH_YEAR])

    def update_pre_conversion(self, df):
        def update_time_string(time_string):
            if time_string.index(':') == 1:
                time_string = '0' + time_string
            return time_string + ':00'

        def get_datetimes(date_column, time_column):
            updated_times = df[time_column].apply(update_time_string)
            updated_dates = df[date_column].apply(reformat_date)
            return updated_dates + " " + updated_times

        df[START_DATETIME] = get_datetimes('StartDate', 'StartTime')
        df[END_DATETIME] = get_datetimes('EndDate', 'EndTime')


## Boston
# From at least February 2018: ['bikeid', 'birth year', 'end station id', 'end station latitude', 'end station longitude', 'end station name', 'gender', 'start station id', 'start station latitude', 'start station longitude', 'start station name', 'starttime', 'stoptime', 'tripduration', 'usertype']
# Starting May 2020: adds 'postal code' and removes ['birth year', 'gender']
BOSTON_CONVERSIONS = {}
BOSTON_CONVERSIONS['starttime'] = START_DATETIME
BOSTON_CONVERSIONS['stoptime'] = END_DATETIME
BOSTON_CONVERSIONS['start station latitude'] = START_LAT
BOSTON_CONVERSIONS['start station longitude'] = START_LONG
BOSTON_CONVERSIONS['end station latitude'] = END_LAT
BOSTON_CONVERSIONS['end station longitude'] = END_LONG
BOSTON_CONVERSIONS['start station name'] = START_STATION_NAME
BOSTON_CONVERSIONS['end station name'] = END_STATION_NAME
BOSTON_CONVERSIONS['birth year'] = BIRTH_YEAR
BOSTON_CONVERSIONS['gender'] = GENDER
BOSTON_CONVERSIONS['usertype'] = IS_SUBSCRIBER

class BostonConverter(Converter):
    def __init__(self):
        super().__init__('boston', BOSTON_CONVERSIONS, [GENDER, BIRTH_YEAR])

## SF
# From at least January 2018: ['bike_id', 'bike_share_for_all_trip', 'duration_sec', 'end_station_id', 'end_station_latitude', 'end_station_longitude', 'end_station_name', 'end_time', 'start_station_id', 'start_station_latitude', 'start_station_longitude', 'start_station_name', 'start_time', 'user_type']
# Adds 'rental_access_method' starting in 2019 for a few nonconsecutive months
# Removes 'bike_share_for_all_trip' starting December 2019
# Starting April 2020: ['end_lat', 'end_lng', 'end_station_id', 'end_station_name', 'ended_at', 'member_casual', 'ride_id', 'rideable_type', 'start_lat', 'start_lng', 'start_station_id', 'start_station_name', 'started_at']
SF_CONVERSIONS = {}
SF_CONVERSIONS['start_time'] = START_DATETIME 
SF_CONVERSIONS['end_time'] = END_DATETIME
SF_CONVERSIONS['start_station_latitude'] = START_LAT
SF_CONVERSIONS['start_station_longitude'] = START_LONG
SF_CONVERSIONS['end_station_latitude'] = END_LAT
SF_CONVERSIONS['end_station_longitude'] = END_LONG
SF_CONVERSIONS['start_station_name'] = START_STATION_NAME
SF_CONVERSIONS['end_station_name'] = END_STATION_NAME
SF_CONVERSIONS['user_type'] = IS_SUBSCRIBER

NEW_SF_COLUMBUS_CONVERSIONS = {}
NEW_SF_COLUMBUS_CONVERSIONS['started_at'] = START_DATETIME
NEW_SF_COLUMBUS_CONVERSIONS['ended_at'] = END_DATETIME
NEW_SF_COLUMBUS_CONVERSIONS['start_lat'] = START_LAT
NEW_SF_COLUMBUS_CONVERSIONS['start_lng'] = START_LONG
NEW_SF_COLUMBUS_CONVERSIONS['end_lat'] = END_LAT
NEW_SF_COLUMBUS_CONVERSIONS['end_lng'] = END_LONG
NEW_SF_COLUMBUS_CONVERSIONS['start_station_name'] = START_STATION_NAME
NEW_SF_COLUMBUS_CONVERSIONS['end_station_name'] = END_STATION_NAME
NEW_SF_COLUMBUS_CONVERSIONS['member_casual'] = IS_SUBSCRIBER

SF_CONVERSIONS.update(NEW_SF_COLUMBUS_CONVERSIONS)

class SfConverter(Converter):
    def __init__(self):
        super().__init__('sf', SF_CONVERSIONS, [GENDER, BIRTH_YEAR])

## Columbus
# February 2018: ['Bike ID', 'Gender', 'Start Station ID', 'Start Station Lat', 'Start Station Long', 'Start Station Name', 'Start Time and Date', 'Stop Station ID', 'Stop Station Lat', 'Stop Station Long', 'Stop Station Name', 'Stop Time and Date', 'User Type', 'Year of Birth']
# Starting March 2018: ['bikeid', 'birthyear', 'end_time', 'from_station_id', 'from_station_location', 'from_station_name', 'gender', 'start_time', 'to_station_id', 'to_station_location', 'to_station_name', 'trip_id', 'tripduration', 'usertype']
# Starting April 2020: ['end_lat', 'end_lng', 'end_station_id', 'end_station_name', 'ended_at', 'member_casual', 'ride_id', 'rideable_type', 'start_lat', 'start_lng', 'start_station_id', 'start_station_name', 'started_at']
COLUMBUS_CONVERSIONS = {}
COLUMBUS_CONVERSIONS['start_time'] = START_DATETIME
COLUMBUS_CONVERSIONS['end_time'] = END_DATETIME
COLUMBUS_CONVERSIONS['Start Station Lat'] = START_LAT
COLUMBUS_CONVERSIONS['Start Station Long'] = START_LONG
COLUMBUS_CONVERSIONS['Stop Station Lat'] = END_LAT
COLUMBUS_CONVERSIONS['Stop Station Long'] = END_LONG
COLUMBUS_CONVERSIONS['Start Station Name'] = START_STATION_NAME
COLUMBUS_CONVERSIONS['from_station_name'] = START_STATION_NAME
COLUMBUS_CONVERSIONS['Stop Station Name'] = END_STATION_NAME
COLUMBUS_CONVERSIONS['to_station_name'] = END_STATION_NAME
COLUMBUS_CONVERSIONS['Year of Birth'] = BIRTH_YEAR
COLUMBUS_CONVERSIONS['birthyear'] = BIRTH_YEAR
COLUMBUS_CONVERSIONS['Gender'] = GENDER
COLUMBUS_CONVERSIONS['gender'] = GENDER
COLUMBUS_CONVERSIONS['User Type'] = IS_SUBSCRIBER
COLUMBUS_CONVERSIONS['usertype'] = IS_SUBSCRIBER

COLUMBUS_CONVERSIONS.update(NEW_SF_COLUMBUS_CONVERSIONS)

class ColumbusConverter(Converter):
    def __init__(self):
        super().__init__('columbus', COLUMBUS_CONVERSIONS, [GENDER, BIRTH_YEAR])

    def update_pre_conversion(self, df):
        def update_date_string(date_string):
            date, time = date_string.split(' ')
            return reformat_date(date) + ' ' + time

        if 'Start Time and Date' in df.columns: # has bad date format: 7/28/2013 04:03:44
            df[START_DATETIME] = df['Start Time and Date'].apply(update_date_string)
            df[END_DATETIME] = df['Stop Time and Date'].apply(update_date_string)

        def update_location(location_col, latitude_col, longitude_col):
            df[latitude_col] = df[location_col].apply(lambda lat_long_pair: lat_long_pair.split(',')[0])
            df[longitude_col] = df[location_col].apply(lambda lat_long_pair: lat_long_pair.split(',')[1])

        if 'from_station_location' in df.columns:
            update_location('from_station_location', START_LAT, START_LONG)
            update_location('to_station_location', END_LAT, END_LONG)

## DC
# From at least February 2018: ['Bike number', 'Duration', 'End date', 'End station', 'End station number', 'Member type', 'Start date', 'Start station', 'Start station number']
# Starting May 2020: ['end_lat', 'end_lng', 'end_station_id', 'end_station_name', 'ended_at', 'member_casual', 'ride_id', 'rideable_type', 'start_lat', 'start_lng', 'start_station_id', 'start_station_name', 'started_at']
# Not including this city because it doesn't have latitude and longitude until recently...

CONVERTERS = {
    # 'boston': BostonConverter(), 
    # 'sf': SfConverter(), 
    # 'nyc': NycConverter(), 
    # 'portland': PortlandConverter(), 
    'columbus': ColumbusConverter()
}
CITIES = CONVERTERS.keys()

def convert_df(df, city):
    if city not in CITIES:
        raise ValueError('{} is not a valid city'.format(city))
    return CONVERTERS[city].convert(df)

# import pandas as pd
# portland1 = pd.read_csv('./bss/portland/2020_07.csv')
# portland2 = pd.read_csv('./bss/portland/2018_02.csv')
# nyc1 = pd.read_csv('./202008-citibike-tripdata.csv')
# boston1 = pd.read_csv('./bss/boston/202001-bluebikes-tripdata.csv')
# sf1 = pd.read_csv('./bss/sf/201901-fordgobike-tripdata.csv')
# sf2 = pd.read_csv('./bss/sf/202006-baywheels-tripdata.csv')
# columbus1 = pd.read_csv('./bss/columbus/201802-cogo-tripdata.csv')
# columbus2 = pd.read_csv('./bss/columbus/201803-cogo-tripdata.csv')
# columbus3 = pd.read_csv('./bss/columbus/202007-cogo-tripdata.csv')