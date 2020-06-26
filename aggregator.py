import boto3
import pandas as pd
from datetime import date, timedelta
from agg_webcam import aggregate as agg_webcam
from agg_hystreet import aggregate as agg_hystreet
from agg_gmap_transit_score import aggregate as agg_gmap_transit_score
from agg_fahrrad import aggregate as agg_fahrrad
from agg_airquality import aggregate as agg_airquality
from agg_lemgo_digital import aggregate as agg_lemgo_digital
#from agg_tomtom import aggregate as agg_tomtom
import json
import settings
import os

if __name__ == "__main__":

    #How far back do you want to aggregate data?
    if "TIMERANGE" in list(os.environ):
        # this can be used for full-range aggregator runs
        # in aws codebuild
        days = int(os.environ["TIMERANGE"])
    else:
        days = 1
    print(f"\nAggregate the last {days} days.")

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

        # print("--------------")
        # print("start tomtom...")
        # try:
            # tomtom_list = pd.DataFrame(agg_tomtom(date))
            # tomtom_list = tomtom_list.set_index('landkreis')
            # list_result = list_result.join(tomtom_list, how="outer")
        # except Exception as e:
            # print("Error Tomtom")
            # print(e)

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

 