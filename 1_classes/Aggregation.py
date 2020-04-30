from datetime import datetime
import boto3
import json
import pandas as pd

class Aggregator:
    """ Static function to aggregate the data frame into a list """
    @staticmethod
    def aggregateDf(df, base, columnIn, columnOut):
        if not "ags" in df:
            print("agg_{}: No 'ags' column in dataframe, skip this date.".format(base))
        else:
            # here we could also grab instead of 'aqi' other values like 'pm25', 'p10', 'no2', 'o3',...
            columnName = "{}.{}".format(base, columnIn)
            print(columnName)
            df[columnName] = pd.to_numeric(df[columnName], errors='coerce')
            df = df.groupby("ags").agg({columnName:"mean"}).reset_index()
            df[columnName] = df[columnName]/100
            df.columns = ["ags",columnOut]
            return json.loads(df.to_json(orient='records'))
    """ Static function to aggregate the json into a list
        equivalent to aggregateDf(pd.json_normalize(jsonString), base, columnIn, columnOut)"""
    @staticmethod
    def aggregateJson(jsonString, base, columnIn, columnOut):
        df = pd.DataFrame(jsonString)
        if not "ags" in df:
            print("agg_{}: No 'ags' column in dataframe, skip this date.".format(base))
        else:
            df[columnIn] = pd.to_numeric(df[base].str[columnIn], errors='coerce')
            df = df.groupby("ags").agg({columnIn:"mean"}).reset_index()
            df[columnIn] = df[columnIn]/100
            df.columns = ["ags",columnOut]
            return json.loads(df.to_json(orient='records'))

    def __init__(self, bucketName="sdd-s3-bucket"):
        self.s3_client = boto3.client('s3')
        self.bucketName = bucketName
    def listFromAWS(self, base, date):
        object_list = []
        # List data
        s3_objects = self.s3_client.list_objects_v2(
            Bucket=self.bucketName,
            Prefix='{}/{}/{}/{}/'.format(
                base,
                str(date.year).zfill(4),
                str(date.month).zfill(2),
                str(date.day).zfill(2)))
        if 'Contents' in s3_objects:
            print("Objects: Found " + str(len(s3_objects['Contents'])) + " elements")
        else:
            print("Objects: Found 0 elements, skip this date.")
            return False
        for key in s3_objects['Contents']:
            object_list.append(key['Key'])
        return object_list
