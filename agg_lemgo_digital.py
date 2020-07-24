import json
from datetime import date, timedelta, datetime

import boto3
import pandas as pd
import settings

from convert_df_to_influxdb import convert_df_to_influxdb
from push_to_influxdb import push_to_influxdb


date_obj = date.today()
date_obj = date.today() - timedelta(30)


def path_to_hour_of_day(path: str):
    strValue = path.split('/')[-1]
    return strValue


def get_relative_traffic(object_body_json):
    traffic_per_hour_object = object_body_json['data']['trafficPerHour']
    traffic_per_hour_dp = pd.DataFrame(traffic_per_hour_object).transpose()
    try:
        traffic_per_hour_dp['relativTraffic'] = pd.to_numeric(traffic_per_hour_dp['trafficNormal']) / pd.to_numeric(
            traffic_per_hour_dp['trafficCurrent'])
    except Exception as e:
        traffic_per_hour_dp['relativTraffic'] = None
        print("relativTraffic issue", e)
    return traffic_per_hour_dp


def get_relative_passerby(object_body_json):
    passerby_per_hour_object = object_body_json['data']['passerbyPerHour']
    passerby_per_hour_dp = pd.DataFrame(passerby_per_hour_object).transpose()
    try:
        passerby_per_hour_dp['relativPasserby'] = pd.to_numeric(passerby_per_hour_dp['passerbyCurrent']) / pd.to_numeric(
            passerby_per_hour_dp['passerbyNormal'])
    except Exception as e:
        passerby_per_hour_dp['relativPasserby'] = None
        print("relativPasserby issue", e)
    return passerby_per_hour_dp


def aggregate(date_obj):
    s3_client = boto3.client('s3')

    s3_objects = s3_client.list_objects_v2(Bucket=settings.BUCKET,
                                           Prefix='lemgo-digital/{}/{}/{}/'.format(str(date_obj.year).zfill(4),
                                                                                   str(date_obj.month).zfill(2),
                                                                                   str(date_obj.day).zfill(2)))
    # if 'Contents' not in s3_objects:
    #     return []

    # print("Found " + str(len(s3_objects['Contents'])) + " elements")
    dict_s3_objects = {}
    for key in s3_objects['Contents']:
        dict_s3_objects[path_to_hour_of_day(key['Key'])] = s3_client.get_object(Bucket=settings.BUCKET,
                                                                                Key=key['Key'])

    latest_lemgo_digital_object = dict_s3_objects[sorted(dict_s3_objects.keys(), reverse=False)[0]]
    object_body = str(latest_lemgo_digital_object["Body"].read(), 'utf-8')

    object_body_json = json.loads(object_body)

    traffic_per_hour_dp = get_relative_traffic(object_body_json)
    # traffic_per_hour_dp.set_index("timestamp")

    passerby_per_hour_dp = get_relative_passerby(object_body_json)
    # passerby_per_hour_dp.set_index("timestamp")

    try:
        aggregated_value = pd.merge(traffic_per_hour_dp, passerby_per_hour_dp, how='outer', on="timestamp")
        aggregated_value.reset_index()
    except Exception as e:
        print("lemgoDigitalAggregated issue", e)
    try:
        aggregated_value['lemgoDigital'] = 0.3 * aggregated_value['relativTraffic'] + 0.7 * aggregated_value[
            'relativPasserby']
    except Exception as e:
        aggregated_value = passerby_per_hour_dp.copy()
        aggregated_value = aggregated_value.rename(columns={"relativPasserby" : "lemgoDigital"})
        # aggregated_value['lemgoDigitalAggregated'] = None
        print("lemgoDigitalAggregated issue", e)

    list_results = []
    try:
        date_minus_one = date_obj - timedelta(days=1)
        #print(aggregated_value["timestamp"])
        #print(str(date))
        aggregated_value_for_day = aggregated_value.loc[aggregated_value['timestamp'] == str(date_minus_one)]
        data_index = {
            'landkreis': '05766',
            'lemgoDigital': aggregated_value_for_day['lemgoDigital'].iloc[0],
            'time': datetime(date_minus_one.year, date_minus_one.month, date_minus_one.day, hour=12).isoformat()
        }
        list_results.append(data_index)
    except Exception as e:
        print(e)

    #print(aggregated_value_for_day)
    list_fields = ["lemgoDigital"]
    list_tags = ["landkreis"]
    aggregated_value['time'] = datetime(date_minus_one.year, date_minus_one.month, date_minus_one.day, hour=12).isoformat()

    aggregated_value['measurement'] = "lemgoDigital"
    aggregated_value['landkreis'] = "05766"
    data = convert_df_to_influxdb(aggregated_value, list_fields=list_fields, list_tags=list_tags)
    push_to_influxdb(data)


    return list_results

#aggregate(date.today() - timedelta(days = 4))
