import boto3
import pandas as pd
import json
from datetime import datetime
import settings

  # - timedelta(days=10)  # only for test purposes
def aggregate(date):
    s3_client = boto3.client('s3')
    data = pd.DataFrame()

    response = s3_client.get_object(Bucket=settings.BUCKET, Key='hystreet/{}/{}/{}'.format(
        str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2)))
    result = pd.DataFrame(json.loads(response["Body"].read()))
    data = data.append(result.loc[result["pedestrians_count"] > 0])

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


    # compute mean for each ags (if multiple stations are in the same landkreis)
    grouped = pd.DataFrame(data_with_ags.groupby(['landkreis'], sort=False)['relative_pedestrians_count'].mean())
    grouped = grouped.reset_index()
    #print(grouped)
    list_results = []

    for index, row in grouped.iterrows():
        data_dict = {
            'hystreet_score': row['relative_pedestrians_count'],
            'landkreis': row['landkreis']
        }
        list_results.append(data_dict)

    return list_results
#aggregate(date.today() - timedelta(days = 2))
