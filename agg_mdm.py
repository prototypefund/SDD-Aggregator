from xml.dom import minidom
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
import io


from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb

def aggregate(date_obj=datetime.date.today()):
    s3_client = boto3.client('s3')
    data = pd.DataFrame()
    try:
        dict_objects = s3_client.list_objects(Bucket=settings.BUCKET, Prefix= 'mdm/{}/{}/{}/'.format(
            str(date_obj.year).zfill(4),
            str(date_obj.month).zfill(2),
            str(date_obj.day).zfill(2)
            )
        )
        for element in dict_objects["Contents"]:
            print(element["Key"])
            response = s3_client.get_object(Bucket=settings.BUCKET, Key=element["Key"])
            body = response["Body"].read()

            buffer = io.StringIO(body.decode())
            mydoc = minidom.parse(buffer)


            list_childnodes = mydoc.childNodes
            for i in list_childnodes:
                print(i.nodeName)
                list_childnodes2 = i.childNodes
                for j in list_childnodes2:
                    print(" ", j.nodeName)
                    list_childnodes3 = j.childNodes
                    for k in list_childnodes3:
                        list_childnodes4 = k.childNodes
                        print("   ", k.nodeName)
                        for l in list_childnodes4:
                            # print("   ", l.nodeName)
                            pass


            payloadPublication = mydoc.getElementsByTagName('payloadPublication')
            for item in payloadPublication:
                print(item)
                value = item.getElementsByTagName('publicationTime')
                for val in value:
                    print(val)
                    val
                print(value)
            body
    except Exception as e:
        print(e)

        print(body)
        # df = pd.DataFrame(json.loads(body))
        # df["date_check"] = date_obj
        # df["hour_check"] = hour
        # df["timestamp_check"] = str(
        #     datetime.datetime(year=date_obj.year, month=date_obj.month, day=date_obj.day, hour=hour))
        # data = data.append(df)
    # except Exception as e:
    #     print(e, key)
    #     pass
    # if data.empty:
    #     print(f"WARNING: No data returned from S3 for {str(date_obj)}!")
    #     return []



    # one specific item attribute
    print('Item #2 attribute:')
    print(items[1].attributes['name'].value)

    # all item attributes
    print('\nAll attributes:')
    for elem in items:
        print(elem.attributes['name'].value)

    # one specific item's data
    print('\nItem #2 data:')
    print(items[1].firstChild.data)
    print(items[1].childNodes[0].data)

    # all items data
    print('\nAll item data:')
    for elem in items:
        print(elem.firstChild.data)


if __name__ == '__main__':
    # for testing
    for i in range(0,4):
        date_obj = date.today() - timedelta(days = i)
        list_results = aggregate(date_obj)
    # print(list_results)
