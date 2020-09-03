from influxdb_client import InfluxDBClient
import os
import rfc3339
import datetime

client = InfluxDBClient(url=os.environ['INF_URL'], token=os.environ['INF_TOKEN'], org=os.environ['INF_ORG'])
delete_api = client.delete_api()

year, month, day = 2020, 1, 1
start_time = datetime.datetime(year,month,day)
year, month, day = 2020, 1, 1
end_time = datetime.datetime.now()

delete_bucket = "testscraperbucket_julius"
predicate_statement = '_measurement="mdm"'

# USE THIS WITH PRECAUTION!!!
delete_api.delete(start=rfc3339.rfc3339(start_time), stop=rfc3339.rfc3339(end_time), predicate=predicate_statement, bucket=delete_bucket, org=os.environ['INF_ORG'])
