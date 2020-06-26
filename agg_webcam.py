import pandas as pd
import datetime
# compatibility with ipython
#os.chdir(os.path.dirname(__file__))
import json
import boto3
from coords_to_kreis import coords_convert
import re
import settings
from influxdb_client import InfluxDBClient

from donotpush import getsettings

# def aggregate_old(date):
#     s3_client = boto3.client('s3')
#     data = pd.DataFrame()
#     for x in range(7,18):
#         try:
#             key = 'webcamdaten/{}/{}/{}/{}webcamdaten.json'.format(str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2), str(x).zfill(2))
#             response = s3_client.get_object(Bucket=settings.BUCKET, Key=key)
#             result = pd.DataFrame(json.loads(response["Body"].read()))
#             result["date"] = date
#             result["hour"] = x
#             data = data.append(result)
#         except Exception as e:
#             print(e,key)
#             pass
#     #print(data)
#     data.columns = [col.lower() for col in data.columns]
#     def extract_float(string):
#         return re.findall("\d+\.\d+", str(string))[0]
#     data["lon"] = data["lon"].apply(extract_float)
#     data["lat"] = data["lat"].apply(extract_float)
#     #print(data.columns)
#     #names(df)[names(df) == 'Lat'] <- 'lat'
#     data["ags"] = coords_convert(data)
#     #return data
#     result = pd.DataFrame(data.groupby("ags")[["personenzahl"]].mean())
#     result = result.reset_index()
#     list_results = []
#     #print(result["personenzahl"])
#     #result["personenzahl"] = result[["personenzahl"]] / 2.4
#     #print(result["personenzahl"])
#     for index, row in result.iterrows():
#         landkreis = row['ags']
#         relative_popularity = row["personenzahl"]
#         data_index = {
#             "landkreis": landkreis,
#             'webcam_score' : relative_popularity
#         }
#         list_results.append(data_index)
#     return list_results
# #aggregate(date.today() - timedelta(days = 2))

def convert_lat_lon_to_float(data):
    try:
        data["lat"] = data["Lon"].astype(float)
        data["lon"] = data["Lat"].astype(float)
        data = data.drop(columns=["Lon", "Lat"])
    except:
        pass
    return data

def transfer_df_to_influxdb(data, list_fields=[], list_tags=[]):
    json_out = []
    for index, row in data.iterrows():
        j = {}
        j["measurement"] = "webcam"
        j["fields"] = {x: row[x] for x in list_fields}
        j["time"] = row["stand"]
        j["tags"] = {}
        for tag in list_tags:
            j["tags"][tag] = row[tag]

        json_out.append(j)
    return json_out

def pushto_influxdb(json_out):
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
    write_api.write(bucket=bucket, org=org, record=json_out)


def aggregate(date=datetime.date.today()):
    date = datetime.date.today().replace(day=12)
    s3_client = boto3.client('s3')
    data = pd.DataFrame()
    for hour in range(7,18):
        try:
            key = 'webcamdaten/{}/{}/{}/{}webcamdaten.json'.format(str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2), str(hour).zfill(2))
            response = s3_client.get_object(Bucket=settings.BUCKET, Key=key)
            body = response["Body"].read()
            df = pd.DataFrame(json.loads(body))
            df["date"] = date
            df["hour"] = hour
            df["timestamp"] = str(datetime.datetime(year=date.year, month=date.month, day=date.day, hour=hour))
            data = data.append(df)
        except Exception as e:
            print(e,key)
            pass
    #print(data)
    # data["columns"] = [col.lower() for col in data.columns]

    data = convert_lat_lon_to_float(data)
    print("dtypes:")
    print("--=--")
    print(data.dtypes)
    print("--=--")

    data["ags"] = coords_convert(data)
    data["ags"] = data["ags"].astype(int,errors="ignore")

    data.columns = [col.lower() for col in data.columns]
    result = pd.DataFrame(data.groupby("ags")["personenzahl"].mean())
    data = data.merge(result)
    data = data.set_index("timestamp")
    list_webcam_fields = ["personenzahl"]
    list_webcam_tags = ["ags", "hour", "lat", "lon", "name"] # TODO: lat lon as tag or not?

    json_out = transfer_df_to_influxdb(data, list_webcam_fields, list_webcam_tags)


    pushto_influxdb(json_out)

# def to_influx(body):
#
#     json_out = {}
#     json_out["tags"] = {}
#     json_out["fields"] = {}
#
#     json_in = json.loads(body)
#     df = pd.DataFrame(json_in)
#     for col in ['id', 'url', 'lat', 'lon', 'name', 'personenzahl', 'stand', 'date', 'hour', 'lat', 'lon', 'geometry', 'ags']:
#         print(col, ":", df[col])
#     timestamp = pd.to_datetime(df["Stand"])
#
#     # timestamp.apply(lambda timestamp: (timestamp - datetime.datetime(1970, 1, 1)).total_seconds())
#     na = timestamp[timestamp.isna()]
#     notna = timestamp[timestamp.notna()]
#     notna = notna.apply(lambda timestamp: (timestamp.timestamp()))
#     timestamp = na.append(notna)
#     df["unix_time"] = timestamp
#
#     # ich mache einen gesamten dataframe daraus um spaltenoperationen für die Zeitspalten usw. durchzuführen...
#     # timestamp wurde zu unix umgewandelt.
#
#
#     # unix_time = (timestamp - datetime.datetime(1970, 1, 1)).total_seconds()
#     json_out["tags"]["unix_time"] = timestamp.to_dict()
#
#
#     for item in json_in:
#         print(item)
#         '''
#         {
#             'ID': 97,
#             'URL': 'https://www.travelcharme.com/fileadmin/contents/02_ohk/ostseehotel-webcam.jpg',
#             'Lat': '54.154391',
#             'Lon': '11.7576813',
#             'Name': 'Strand Kühlungsborn',
#             'Personenzahl': 0,
#             'Stand': '2020-06-12 17:33'
#          }
#         '''
#         j = {}
#         j["tags"] = {}
#         j["measurement"] = "webcam"
#         # j["tags"] = {x: item[x] for x in ["ID", "Name", "URL", "Stand"]}
#
#         # j["fields"] = {x: to_float(item[x]) for x in ["pedestrians_count", "temperature", "min_temperature"]}
#
#
#         # j["time"] = int(parse(item["timestamp"]).timestamp()) * 1000000000
#
#         ags = stationsdict[item["station_id"]]["landkreis"]
#         j["tags"]["ags"] = ags
#         j["tags"]["landkreis"], j["tags"]["bundesland"] = ags2landkreis[ags]
#         for tag in ["name", "city"]:
#             j["tags"][tag] = stationsdict[item["station_id"]][tag]
#         for field in ["lat", "lon"]:
#             j["fields"][field] = stationsdict[item["station_id"]][field]
#
#         json_out.append(j)
#     print(len(json_out))
#     print(json.dumps(json_out[-1], indent=2, sort_keys=True))