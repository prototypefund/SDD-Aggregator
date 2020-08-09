import pandas as pd
import datetime
from datetime import date, timedelta
# compatibility with ipython
#os.chdir(os.path.dirname(__file__))
import json
import boto3
from coords_to_kreis import coords_convert, get_ags
import re
import settings


from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb



def convert_lat_lon_to_float(data):
    try:
        data["lat"] = data["Lat"].astype(float)
        data["lon"] = data["Lon"].astype(float)
        data = data.drop(columns=["Lon", "Lat"])
    except:
        print("convertlatlonerror")
        pass
    return data



def aggregate(date_obj=datetime.date.today()):
    s3_client = boto3.client('s3')
    data = pd.DataFrame()
    list_keys = [""]
    for hour in range(0,23):
        try:
            key = 'webcamdaten/{}/{}/{}/{}webcamdaten-customvision.json'.format(str(date_obj.year).zfill(4), str(date_obj.month).zfill(2), str(date_obj.day).zfill(2), str(hour).zfill(2))
            response = s3_client.get_object(Bucket=settings.BUCKET, Key=key)
            body = response["Body"].read()
            json_body = json.loads(body)
            #maybe better... if k in list_keys
            json_body = [{k: v for k, v in d.items() if k != "pred"} for d in json_body]

            df = pd.DataFrame(json_body)
            df["date_check"] = date_obj
            df["hour_check"] = hour
            df["timestamp_check"] = str(datetime.datetime(year=date_obj.year, month=date_obj.month, day=date_obj.day, hour=hour))
            data = data.append(df)
        except Exception as e:
            print(e,key)
            pass
    if data.empty:
        print(f"WARNING: No data returned from S3 for {str(date_obj)}!")
        return []

    data = convert_lat_lon_to_float(data)
    data = get_ags(data)
    data.columns = [col.lower() for col in data.columns]
    # data["ags"] = data["ags"].astype(int, errors="ignore")
    data["personenzahl"] = data["personenzahl"].astype(float, errors="raise")
    data["measurement"] = "webcam-customvision"
    
    # cannot use "id" as unique identifier, because e.g. id=35 was assigned to multiple 
    # cameras in the past by accident. Workaround: use compound _id made up from id and ags.
    data["_id"] = data.apply(lambda x: str(x["id"])+"_"+str(x["ags"]), 1)

    if "orgin" not in data.columns:
        data["origin"] = data["URL"]
    data = data.rename(columns={"stand": "time",
                                "state": "bundesland",
                                "districttype": "districtType"})
    list_webcam_fields = ["personenzahl", "lat", "lon"]
    list_webcam_tags = [
        '_id',
        'name',
        'ags',
        'bundesland',
        'landkreis',
        'districtType',
        'origin']
    json_out = convert_df_to_influxdb(data, list_webcam_fields, list_webcam_tags)
    push_to_influxdb(json_out)
    print(json_out)
    
    # prepare output for aggregator
    data = pd.DataFrame(data.groupby("ags").mean())  # aggregate per day
    data = data.reset_index()
    list_results = []
    for index, row in data.iterrows():
        landkreis = row['ags']
        webcam_score = row["personenzahl"]
        data_index = {
            "landkreis": landkreis,
            'webcam_customvision_score': webcam_score
        }
        list_results.append(data_index)
    return list_results
#
# if __name__ == '__main__':
#     # for testing
#     for i in range(1,14):
#         date = date.today() - timedelta(days = i)
#         list_results = aggregate(date)
#     print(list_results)
