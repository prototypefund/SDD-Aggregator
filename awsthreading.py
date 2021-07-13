import boto3
import settings
import io
import threading
from xml.dom import minidom
import datetime


def get_object(s3_client, element):
    response = s3_client.get_object(Bucket=settings.BUCKET, Key=element)
    body = response["Body"].read()
    return io.StringIO(body.decode())


def get_client():
    return boto3.client('s3')


class AWSRequest(threading.Thread):
    def __init__(self, list_keys):
        threading.Thread.__init__(self)
        self.list_keys = list_keys
        # self.s3_client = boto3.client('s3')
        # self.list_doc = []
        self.dict_doc = {}

    def run(self):
        for element in self.list_keys:
            self.get_object(element)

    def get_object(self, element):
        # print("0_REQUEST  ", element)
        response = boto3.client('s3').get_object(Bucket=settings.BUCKET, Key=element)
        body = response["Body"].read()
        # print("1_RESPONSE  ", element)
        # self.list_doc.append({element : minidom.parse(io.StringIO(body.decode()))})
        self.dict_doc[element] = minidom.parse(io.StringIO(body.decode()))

def create_threads(list_keys=[], num_th = 200):
    list_threads = []
    num_th += 1
    len_list_keys = len(list_keys)
    if len_list_keys > num_th:
        for id in range(1, num_th):
            list_sub = []
            thr = len_list_keys - len_list_keys * id / num_th
            while len(list_keys) > thr:
                # print(len_list_keys)
                x = list_keys.pop(0)
                list_sub.append(x)
            list_threads.append(AWSRequest(list_sub))
    else:
        list_threads.append(AWSRequest(list_keys))
    print("threads appended")
    for t in list_threads:
        t.start()
    print("threads started")
    for t in list_threads:
        t.join()
    print("threads joined")
    return list_threads


def get_mdm_prefix(date_obj):
    prefix = 'mdm/{}/{}/{}/'.format(
        str(date_obj.year).zfill(4),
        str(date_obj.month).zfill(2),
        str(date_obj.day).zfill(2)
    )
    try:
        prefix += str(date_obj.minute).zfill(2) + "/"
    except Exception as e:
        pass
    return prefix


def _init():
    date_obj = datetime.datetime.now()
    date_obj = date_obj.replace(minute=int(date_obj.minute / 15) * 15)
    date_obj = date_obj.replace(minute=15)
    date_obj = date_obj - datetime.timedelta(days=1)
    date_obj = datetime.datetime(2020, 8, 6)
    return date_obj


def get_mdm_data(date_obj):
    s3_client = boto3.client('s3')
    dict_objects = s3_client.list_objects(Bucket=settings.BUCKET, Prefix=get_mdm_prefix(date_obj))
    print(dict_objects)
    list_keys = [x["Key"] for x in dict_objects["Contents"]]
    list_threads = create_threads(list_keys, num_th=100)
    print(list_threads)
    dict_doc = {}
    for t in list_threads:
        dict_doc.update(t.dict_doc)
    print(dict_doc)
    # dict_doc = [minidom.parse(x) for x in list_doc]
    print(dict_doc)
    return dict_doc

# if __name__ == "__main__":
#     date_obj = _init()
#     mdm_data = get_mdm_data(date_obj)
