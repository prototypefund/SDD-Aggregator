import boto3
import pandas as pd
import json
from datetime import datetime, timedelta, date
from coords_to_kreis import coords_convert

  # - timedelta(days=10)  # only for test purposes
def aggregate(date):
    s3_client = boto3.client('s3')
    data = pd.DataFrame()
    clientFirehose = boto3.client('firehose')

    response = s3_client.get_object(Bucket='sdd-s3-basebucket', Key='hystreet/{}/{}/{}'.format(
        str(date.year).zfill(4), str(date.month).zfill(2), str(date.day-3).zfill(2)))
    result = pd.DataFrame(json.loads(response["Body"].read()))
    data = data.append(result)


    def compute_weekday(timestamp):
        date_str = timestamp.split('+')[0]
        date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
        return date.weekday()


    data['weekday'] = float("NaN")
    for index, row in data.iterrows():
        data.at[index, 'weekday'] = compute_weekday(row['timestamp'])

    station_means = pd.read_csv('station_means.csv')

    data = pd.merge(data, station_means, left_on=['station_id', 'weekday'],
                    right_on=['station_id_mean', 'weekday_mean'], how='left').drop(['station_id_mean', 'weekday_mean'], axis=1)

    data['relative_pedestrians_count'] = float("NaN")
    for index, row in data.iterrows():
        data.at[index, 'relative_pedestrians_count'] = row['pedestrians_count'] / \
            row['mean_pedestrians_count_weekday']


    stations_with_ags = pd.read_csv('data/stations_with_ags.csv')
    data_with_ags = pd.merge(data, stations_with_ags, left_on='station_id',
                             right_on='stationid', how='left').drop('stationid', axis=1)
    data_with_ags['landkreis'] = coords_convert(data_with_ags)


    # compute mean for each ags (if multiple stations are in the same landkreis)
    grouped = data_with_ags.groupby(['ags', 'date'], sort=False).agg(
        {'pedestrians_count': 'mean', 'min_temperature': 'mean', 'temperature': 'mean', 'weather_condition': lambda x: x.iloc[0], 'relative_pedestrians_count': 'mean', 'city': lambda x: x.iloc[0], 'lat': lambda x: x.iloc[0], 'lon': lambda x: x.iloc[0], 'landkreis': lambda x: x.iloc[0]}).reset_index()

    list_results = []

    for index, row in grouped.iterrows():
        data_dict = {
            #'name': row['city'],
            'hystreet_score': row['relative_pedestrians_count'],
            'landkreis': row['landkreis']
            #'lat': row['lat'],
            #'lon': row['lon'],
            #'date': row['date']
        }
        list_results.append(data_dict)

    return list_results
