from xml.dom import minidom
import datetime

def rec(el, recursion_lvl):
    spaces = recursion_lvl * "   "
    for node in el.childNodes:
        print(f"{spaces}NAME: ", node.nodeName)
        print(f"{spaces}VALUE: ", node.nodeValue)
        print(f"{spaces}PARENT: ", node.parentNode.nodeName)
        print(f"{spaces}CHILDS: ", node.hasChildNodes())
        print("- - -")
        if node.parentNode.nodeName == "basicData":
            pass
            # return node
        if node.nodeName == "predefinedLocationReference":
            node.getAttributeNode("id").value
            node.getAttributeNode("targetClass").value
            node.parentNode.getAttributeNode("xsi:type").value
            return node
    # df.join(rec())
    # df = pd.DataFrame()
    # for i in range(5):
    #     rec(el, )

def recursive(el, recursion_lvl):
    spaces = recursion_lvl * "   "
    for node in el.childNodes:
        print(f"{spaces}NAME: ", node.nodeName)
        print(f"{spaces}VALUE: ", node.nodeValue)
        print(f"{spaces}PARENT: ", node.parentNode.nodeName)
        print(f"{spaces}CHILDS: ", node.hasChildNodes())
        print("- - -")
        if node.parentNode.nodeName == "basicData":
            pass
            # return node
        if node.nodeName == "predefinedLocationReference":
            node.getAttributeNode("id").value
            node.getAttributeNode("targetClass").value
            node.parentNode.getAttributeNode("xsi:type").value
            return node


        # if node.parentNode.nodeName == "basicData" and node.parentNode.hasChildNodes():
        #     return node

        rec = recursive(node, recursion_lvl + 1)
        if rec:
            return rec

def aggregate():
    pass

def create_df(mdm_data):
    columns = ["Rang","key","isnode","value_dtype","value","isinnode","isrelevant", "document"]
    df = pd.DataFrame(columns=columns)
    for d in mdm_data:
        rec(d)


import pandas as pd
from awsthreading import get_mdm_data, _init, get_client, get_mdm_prefix
if __name__ == "__main__":
    date_obj = _init()
    date_obj = datetime.datetime.now()
    dict_objects = get_client().list_objects(Bucket=settings.BUCKET, Prefix=get_mdm_prefix(date_obj))
    date_obj = date_obj.replace(minute=int(date_obj.minute/15) *15)
    data = pd.DataFrame()
    date_obj = datetime.date.today() - datetime.timedelta(1)
    mdm_data = get_mdm_data(date_obj)
    df = create_df(mdm_data)


# import boto3
# import settings
# import io
# import sys

# def get_data(date_obj):
#     s3_client = boto3.client('s3')
#     dict_objects = s3_client.list_objects(Bucket=settings.BUCKET, Prefix=get_prefix(date_obj))
#     list_doc = []
#     try:
#         for element in dict_objects["Contents"]:
#             response = s3_client.get_object(Bucket=settings.BUCKET, Key=element["Key"])
#
#             body = response["Body"].read()
#             buffer = io.StringIO(body.decode())
#             list_doc.append(minidom.parse(buffer))
#     except Exception as e:
#         pass
#         # return None, e
#     return list_doc, dict_objects


