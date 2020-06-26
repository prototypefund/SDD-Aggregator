import json
import boto3
from coords_to_kreis import get_ags
from datetime import date,datetime,timedelta
import pandas as pd
import settings
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import transfer_df_to_influxdb
from rfc3339 import rfc3339

def aggregate(date):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=settings.BUCKET, Key='fahrrad/{}/{}/{}/{}.json'.format(str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2), str(date)))

    df = pd.DataFrame(json.loads(json.loads(response["Body"].read())))
    df = get_ags(df)
    df = df.loc[~df["ags"].isin(["05362", "05913"])]
    df["bike_count"] = df["bike_count"].astype(int)
    df["prediction"] = df["prediction"].astype(int)
    df["score"] = df["bike_count"] / df["prediction"]
    result = pd.DataFrame(df.groupby("ags")["score"].mean())
    result = result.reset_index()
    
    # push to influxdb
    df = df.reset_index()
    df["timestamp"] = df.apply(lambda x: 1000000000*int(datetime.timestamp((pd.to_datetime(x["date"])))),1)
    df["measurement"] = "bikes"
    df["origin"] = "https://www.eco-compteur.com/"
    df = df.rename(columns={
        'index':'_id', 
        'state':'bundesland'
    })
    list_fields = [
        'lat', 
        'lon',
        'bike_count',
        'temperature',
        'precipitation',
        'snowdepth', 
        'windspeed', 
        'sunshine',
        'prediction',
        'score',
        ]
    list_tags = [
        '_id',
        'name',
        'ags',
        'bundesland',
        'landkreis',
        'districtType']
    df[list_fields] = df[list_fields].astype(float)
    df['ags'] = pd.to_numeric(df['ags'])
    json_out = transfer_df_to_influxdb(df,list_fields,list_tags)
    #import pdb; pdb.set_trace()
    push_to_influxdb(json_out)
    
    # prepare output for aggregator
    list_results = []

    for index, row in result.iterrows():
        landkreis = row['ags']
        relative_popularity = row["score"]
        data_index = {
            "landkreis": landkreis,
            'bike_score' : relative_popularity
        }
        list_results.append(data_index)
    return list_results
    
   
if __name__ == '__main__':
    # for testing
    for i in range(1,14):
        date = date.today() - timedelta(days = i)
        list_results = aggregate(date)
    print(list_results)
    