from coords_to_kreis import get_ags
import boto3
import json
import time
from datetime import date,datetime,timedelta,time
import pandas as pd
import csv
import numpy as np
import settings
import ast
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb

def aggregate(date):
    s3_client = boto3.client('s3')
    df = pd.DataFrame()

    for x in range(9,19):
        try:
            response = s3_client.get_object(Bucket=settings.BUCKET, Key='googleplaces/{}/{}/{}/{}'.format(str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2), str(x).zfill(2)))
            result = pd.DataFrame(json.loads(response["Body"].read()))
            result["date"] = date
            result["hour"] = x
            result["datetime"] = datetime.combine(date,time(x))
            df = df.append(result)
        except Exception as e:
            print("  No gmap data for " + str(x) + ":00 " + str(date) + " " + str(e))
    if df.empty:
        print(f"WARNING: No data returned from S3 for {str(date)}!")
        return []

    def normal_popularity(row):
        return row["populartimes"][row["date"].weekday()]["data"][row["hour"]]

    df["normal_popularity"] = df.apply(normal_popularity, axis = 1, result_type = "reduce")
    df=df[df["normal_popularity"]!=0]
    df["relative_popularity"] = df["current_popularity"] / df["normal_popularity"]
    df["coordinates"] = df["coordinates"].astype(str)
    lat = []
    lon = []
    for index, row in df.iterrows():
        lat.append(ast.literal_eval(row["coordinates"])["lat"])
        lon.append(ast.literal_eval(row["coordinates"])["lng"])

    df["lat"] = lat
    df["lon"] = lon
    df = get_ags(df)

    # push to influxdb
    df = df.reset_index()
    df["time"] = df.apply(lambda x: 1000000000*int(datetime.timestamp((pd.to_datetime(x["datetime"])))),1)
    df["measurement"] = "google_maps"
    baseurl = "https://www.google.com/maps/place/?q=place_id:"
    df["origin"] = df.apply(lambda x: baseurl+x["id"],1)
    df = df.rename(columns={
        'id':'_id', 
        'state':'bundesland',
    })
    list_fields = [
        'lat', 
        'lon',
        'current_popularity',
        'normal_popularity',
        ]
    list_tags = [
        '_id',
        'name',
        'ags',
        'bundesland',
        'landkreis',
        'districtType',
        'origin']
    df[list_fields] = df[list_fields].astype(float)
    df['ags'] = pd.to_numeric(df['ags'])
    json_out = convert_df_to_influxdb(df, list_fields, list_tags)
    push_to_influxdb(json_out)

    # prepare output for aggregator
    result = df.loc[df["ags"].notna()]
    result = result.groupby("ags").apply(lambda x: np.average(x.relative_popularity, weights=x.normal_popularity))
    result = result.reset_index()
    result.columns = ["ags", "relative_popularity"]
    list_results = []
    for index, row in result.iterrows():
        ags = row['ags']
        relative_popularity = row['relative_popularity']
        data_index = {
            "landkreis": ags,
            "gmap_score" : relative_popularity
        }
        list_results.append(data_index)
    return list_results

if __name__ == '__main__':
    # for testing
    for i in range(1,14):
        date = date.today() - timedelta(days = i)
        list_results = aggregate(date)
    print(list_results)
