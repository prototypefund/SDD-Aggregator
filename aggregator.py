import boto3
import pandas as pd
from datetime import date, timedelta
from agg_webcam import aggregate as agg_webcam
from agg_hystreet import aggregate as agg_hystreet
from agg_gmap_supermarket_score import aggregate as agg_gmap_supermarket_score
from agg_gmap_transit_score import aggregate as agg_gmap_transit_score
from agg_zugdaten import aggregate as agg_zugdaten
from agg_fahrrad import aggregate as agg_fahrrad
from agg_airquality import aggregate as agg_airquality
from agg_lemgo_digital import aggregate as agg_lemgo_digital
from agg_tomtom import aggregate as agg_tomtom
import json
import settings

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS

if __name__ == "__main__":

    #How far back do you want to aggregate data?
    days = 1

    s3_client = boto3.client('s3')

    for x in range(0,days):
        date = date.today() - timedelta(days = x)
        print("\n##########################")
        print('###   START ',date,"\n")
        list_result = pd.DataFrame(columns = ['landkreis'])
        list_result = list_result.set_index("landkreis")

        print("start lemgo...")
        try:
            lemgo_digital_list = pd.DataFrame(agg_lemgo_digital(date))
            lemgo_digital_list = lemgo_digital_list.set_index('landkreis')
            list_result = list_result.join(lemgo_digital_list, how="outer")
        except Exception as e:
            print("Error Lemgo:")
            print(e)

        print("--------------")
        print("start gmap...")
        try:
            gmapscore_list = pd.DataFrame(agg_gmap_transit_score(date))
            gmapscore_list = gmapscore_list.set_index('landkreis')
            list_result = list_result.join(gmapscore_list, how="outer")
        except Exception as e:
            print("Error GMAP:")
            print(e)

        print("--------------")
        print("start webcams...")
        try:
            webcam_list = pd.DataFrame(agg_webcam(date))
            webcam_list = webcam_list.set_index('landkreis')
            list_result = list_result.join(webcam_list, how="outer")
        except Exception as e:
            print("Error Webcam")
            print(e)
        print("--------------")
        print("start hystreet...")
        try:
            hystreet_list = pd.DataFrame(agg_hystreet(date))
            hystreet_list = hystreet_list.set_index('landkreis')
            list_result = list_result.join(hystreet_list, how = "outer")
        except Exception as e:
            print("Error Hystreet")
            print(e)

        print("--------------")
        print("start fahrrad...")
        try:
            fahrrad_list = pd.DataFrame(agg_fahrrad(date))
            fahrrad_list = fahrrad_list.set_index('landkreis')
            list_result = list_result.join(fahrrad_list, how="outer")
        except Exception as e:
            print("Error Fahrrad")
            print(e)

        print("--------------")
        print("start airquality...")
        try:
            airquality_list = agg_airquality(date)
            if airquality_list == []:
                print("airquality: No data")
            else:
                airquality_df = pd.DataFrame(airquality_list)
                airquality_df = airquality_df.set_index('ags')
                list_result = list_result.join(airquality_df, how="outer")
        except Exception as e:
            print("Error Airquality")
            print(e)

        print("--------------")
        print("start tomtom...")
        try:
            tomtom_list = pd.DataFrame(agg_tomtom(date))
            tomtom_list = tomtom_list.set_index('landkreis')
            list_result = list_result.join(tomtom_list, how="outer")
        except Exception as e:
            print("Error Tomtom")
            print(e)

        print("--------------")
        print("write output...")
        list_result["date"] = str(date)
        #list_result.to_csv("test.csv")

        #list_result
        dict = list_result.T.to_dict()
        #dict
        # s3_client.put_object(Bucket='sdd-s3-basebucket', Key="aggdata/live", Body=json.dumps(dict))
        response = s3_client.put_object(Bucket=settings.BUCKET, Key='aggdata/{}/{}/{}'.format(str(date.year).zfill(4), str(date.month).zfill(2),str(date.day).zfill(2)), Body=json.dumps(dict))
        print("s3_client.put_object response:",response)
        print('\n###     END ',date,"")
        print("##########################\n")

        class List_element:
            def __init__(self, ):


        for list_el in list_collection:
            list_el

def pushto_influxdb():
    from donotpush import getsettings
    event = {
        "output_format": "csv",
        "bucket": "test"
    }
    check, userdata = getsettings(event["bucket"], event["output_format"])

    url = userdata["url"]
    user = userdata["user"]
    token = userdata["token"]
    pw = userdata["pw"]
    org = userdata["org"]
    bucket = userdata["bucket"]
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api()

    write_api.write(bucket=bucket,org=org,)


import pandas as pd

csv_url = "https://github.com/socialdistancingdashboard/SDD-Aggregator/raw/master/data/stations_with_ags.csv"
df = pd.read_csv(csv_url)
stationsdict = df.drop_duplicates().set_index("stationid").to_dict('index')
stationsdict

# *Note*: Grafana has issues with long (int) values, therefore convert all fields to float.

import boto3
import datetime
import json
from dateutil.parser import parse
from tqdm.auto import tqdm


def to_float(x):
    if x == None:
        return None
    else:
        return float(x)


ags_names = pd.read_csv("ags_names.csv", delimiter=";", dtype={"ags": "int"})
ags2landkreis = {x[0]: (x[1], x[2]) for x in zip(ags_names["ags"], ags_names["landkreis"], ags_names["bundesland"])}

# GET DATA FROM S3
daysback = 180

s3_client = boto3.client('s3')
bucket = "sdd-s3-bucket"
json_out = []
for days in tqdm(range(1, daysback)):
    date = datetime.date.today() - datetime.timedelta(days=days)
    # print(date)
    key = 'hystreet/{}/{}/{}'.format(str(date.year).zfill(4),
                                     str(date.month).zfill(2),
                                     str(date.day).zfill(2))
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_in = json.loads(response["Body"].read())
    for item in json_in:
        j = {}
        j["measurement"] = "hystreet"
        j["tags"] = {x: item[x] for x in ["station_id", "unverified"]}
        j["fields"] = {x: to_float(item[x]) for x in ["pedestrians_count", "temperature", "min_temperature"]}
        j["time"] = int(parse(item["timestamp"]).timestamp()) * 1000000000

        ags = stationsdict[item["station_id"]]["landkreis"]
        j["tags"]["ags"] = ags
        j["tags"]["landkreis"], j["tags"]["bundesland"] = ags2landkreis[ags]
        for tag in ["name", "city"]:
            j["tags"][tag] = stationsdict[item["station_id"]][tag]
        for field in ["lat", "lon"]:
            j["fields"][field] = stationsdict[item["station_id"]][field]

        json_out.append(j)
    print(len(json_out))
    print(json.dumps(json_out[-1], indent=2, sort_keys=True))

    # WRITE DATA TO INFLUXDB

    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    # EveryoneCounts
    url = "http://ec2-18-184-168-48.eu-central-1.compute.amazonaws.com:9999/"
    token = ""  # <<<<< YOU NEED TO PROVIDE A TOKEN HERE
    org = "ec"
    bucket = "test-hystreet"

    client = InfluxDBClient(url=url, token=token, org=org)

    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket, org, json_out, time_precision='ms')