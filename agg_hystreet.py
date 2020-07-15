import boto3
import pandas as pd
import json
from datetime import date, datetime, timedelta
import settings
from coords_to_kreis import get_ags
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb


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

    stations_with_ags = pd.read_csv('data/stations_with_ags.csv',dtype={"landkreis":"str"})
    data_with_ags = pd.merge(data, stations_with_ags, left_on='station_id',
                             right_on='stationid', how='left').drop('stationid', axis=1)
    # Check for unknown station_ids
    ids_in_data = set(data["station_id"])
    known_ids = set(stations_with_ags["stationid"])
    for unknown_id in ids_in_data.difference(known_ids):
        print(f"Warning: Unknown hystreet station_id: {unknown_id}.")
        print(f"         Updating stations_with_ags.csv by running make_stations_with_ags.py should fix this.")

    # Drop unknown stations
    unknown_index = data_with_ags[data_with_ags["name"].isna()].index
    data_with_ags = data_with_ags.drop(unknown_index)

    # compute mean for each ags (if multiple stations are in the same landkreis)
    grouped = pd.DataFrame(data_with_ags.groupby(['landkreis'], sort=False)['relative_pedestrians_count'].mean())
    grouped = grouped.reset_index()
    list_results = []

    for index, row in grouped.iterrows():
        data_dict = {
            'hystreet_score': row['relative_pedestrians_count'],
            'landkreis': row['landkreis']
        }
        list_results.append(data_dict)
    
    # push to influxdb
    data_with_ags = prepare_for_influxdb(data_with_ags)
    list_fields = [
        'lat', 
        'lon',
        'pedestrian_count',
        'min_temperature',
        'temperature']
    list_tags = [
        '_id',
        'unverified',
        'name',
        'city',
        'ags',
        'bundesland',
        'landkreis',
        'districtType',
        'origin']
    json_out = convert_df_to_influxdb(data_with_ags, list_fields, list_tags)
    push_to_influxdb(json_out)

    return list_results


def prepare_for_influxdb(df):
    """
    Bring a dataframe in the right format for
    the push_to_influxdb function
    """
    df = df.rename(columns={
        'landkreis': 'ags'
    })
    df = get_ags(df)
    df["time"] = df.apply(lambda x: 1000000000*int(datetime.timestamp((pd.to_datetime(x["timestamp"])))), 1)
    df["measurement"] = "hystreet"
    df["origin"] = "https://hystreet.com"
    df = df.rename(columns={
        'station_id': '_id',
        'pedestrians_count': 'pedestrian_count',
        'state': 'bundesland'
    })
    df['ags'] = pd.to_numeric(df['ags'])
    # import pdb; pdb.set_trace()
    return df


if __name__ == '__main__':
    # for testing
    for i in range(1, 5):
        date = date.today() - timedelta(days = i)
        list_results = aggregate(date)
    print(list_results)
    
    



