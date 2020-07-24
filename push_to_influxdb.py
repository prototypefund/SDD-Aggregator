from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import os

try:
    INF_TOKEN = os.environ['INF_TOKEN']
    #INF_BUCKET = os.environ['INF_BUCKET']
    INF_URL = os.environ['INF_URL']
    INF_ORG = os.environ['INF_ORG']
except KeyError as e:
    try:
        from donotpush import getsettings

        err, temp = getsettings("dev", "csv")
        INF_TOKEN = temp["token"]
        #INF_BUCKET = temp["bucket"]
        INF_URL = temp["url"]
        INF_ORG = temp["org"]
    except:
        pass
    print("switched to local mode")


def push_to_influxdb(json_out, write_bucket="sdd"):
    client = InfluxDBClient(url=INF_URL, token=INF_TOKEN, org=INF_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=write_bucket, org=INF_ORG, record=json_out)
