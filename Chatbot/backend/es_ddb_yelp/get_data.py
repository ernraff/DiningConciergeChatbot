import json
import logging
import boto3
from decimal import Decimal
import requests
import datetime
from botocore.exceptions import ClientError
logger = logging.getLogger(__name__)
from boto3.dynamodb.conditions import Key
import botocore
from botocore.config import Config
import requests
# from requests_aws4auth import AWS4Auth

region = 'us-east-1' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()

print("show me credential:", credentials)

# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

dynamodb = boto3.resource('dynamodb', region)
table = dynamodb.Table("yelp-restaurants")


data = ['id', 'name', 'review_count', 'rating', 'coordinates', 'address1', 'zip_code', 'phone']

host = 'https://search-restaurants-wchgwotnvkcsqoff7wthugxfqq.us-east-1.es.amazonaws.com'
index = 'restaurants'
datatype = 'Restaurant'

url = host + '/' + index + '/' + datatype + '/'
headers = { "Content-Type": "application/json" }
es_data = ['id']


def get_restaurant(id):

    response = table.query(
    KeyConditionExpression=Key('id').eq(id))

    if len(response["Items"])>0:
        return True
    else:
        return False


def write_db(response, cuisine):
    json_response = json.loads(response.text, parse_float=Decimal)

    for t in json_response["businesses"]:


        ## we need to take values and key that exist in data 
        storeCollection = {}
        elsSearch = {}

        idKey = t['id']

        if get_restaurant(idKey) == True:
            continue

        for key, value in t.items():
            if key in data:
                storeCollection.update({key:value})
            elif key == "location":
                for locationKey, locationValue in t[key].items():
                    if locationKey in data:
                        storeCollection.update({locationKey:locationValue})
            if key in es_data:
                elsSearch.update(id=idKey)
                elsSearch.update(cuisine=cuisine)
                


        storeCollection.update(cuisine=cuisine)
        timeStamp=str(datetime.datetime.now())
        itemDictionary = {}
        itemDictionary.update(storeCollection)
        itemDictionary.update(insertedAtTimestamp=timeStamp)

        elsSearchJSON=json.loads(json.dumps(elsSearch))

        r = requests.post(url+str(idKey), json=elsSearchJSON, headers=headers)
        print("r:",r)

        table.put_item(Item=itemDictionary)
    
    print(table)


def lambda_handler(event=None, context=None):
    
    cuisines=['chinese', 'korean', 'american', 'japanese', 'thai']



    for cuisine in cuisines:
        params = {
            'limit': 50, 
            'location': ('nyc'),
            'term': cuisine,
            'businesses': 'restaurant'
        }
        url = 'https://api.yelp.com/v3/businesses/search' 
        payload = {}
        headers = {
            'Authorization': 'Bearer NAt0CkP0_KgW9GEq6UhDlV0u2jMfSCoxV-2LGPOWBJmCQelE0qz_WgUU9PXq0b_DkSrV4vYwc-_C87tD2sfaGmenc_7-hBK4HHMTJ677bL5QGzrVqWvbZmvn-vcAZHYx',
            'Cookie': 'wdi=2|DBFD188E9CE95394|0x1.90011f668ce5bp+30|deb9ebc749946999'
        }

        response = requests.request("GET", url, headers=headers, data=payload, params=params)
        write_db(response,cuisine)

    return None

