import json
import boto3
import sys
import requests
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sns = boto3.client('sns', 'us-east-1')
sqs = boto3.resource('sqs', 'us-east-1')
dynamodb = boto3.resource('dynamodb', 'us-east-1')
table_name = "yelp-restaurants"
region = 'us-east-1'
service = 'es'
index = 'restaurants'

host = 'https://search-restaurants-wchgwotnvkcsqoff7wthugxfqq.us-east-1.es.amazonaws.com'
url = host + '/' + index + '/_search?'
queue = sqs.get_queue_by_name(QueueName='Q1')


def get_recommendations(request):
    table = dynamodb.Table(table_name)

    headers = headers = {
        'Content-Type': "application/json",
        'Accept': "/",
        'Cache-Control': "no-cache",
        'Host': host,
        'Accept-Encoding': "gzip, deflate",
        'Content-Length': "335",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    cuisine = request['cuisine']

    searchParams = {
        "from": 0,
        "size": 3,
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "cuisine": cuisine
                    }
                },
                "random_score": {}
            }
        }
    }
    
    response = requests.get(url=url, data=json.dumps(searchParams), headers=headers)
    elasticSearchResponse = json.loads(response.text)
    recommendations = []

    for i in elasticSearchResponse["hits"]["hits"]:
        x = table.query(KeyConditionExpression=Key('restaurant_id').eq(i.get("_id")))
        resName = x['Items'][0]['name']

        resAddress = " ".join(x['Items'][0]['display_address'])
        recommendations.append({'name': resName, 'address': resAddress})
    return recommendations


def messageTemplate(request, msgContent):
    dataCuisine = request['cuisine']
    dataPeople = request['number_of_people']
    dataTime = request['dining_time']

    content = 'Hey! Here are my recommendations for  ' + dataCuisine + \
        dataPeople + ' people, on ' + ' at ' + dataTime + '.\n\n'
    for i in range(len(msgContent)):
        content += (str(i+1) + '. ' +
                    msgContent[i]['name'] + ', located at ' + msgContent[i]['address'])
        content += '\n'
    print(content)
    return content


def send_to_sns(sharedData, instanceData):

    msg = messageTemplate(sharedData, instanceData)
    dataPhone = "+1" + sharedData['phone_number']
    
    print("I am sending you TEXT MESSAGE!")

    sns.publish(
        PhoneNumber=dataPhone,
        Message=msg)

def delete_message(message):
    try:
        message.delete()
    except ClientError as error:
        raise error


def delete_messages(queue, messages):
    try:
        entries = [{
            'Id': str(ind),
            'ReceiptHandle': msg.receipt_handle
        } for ind, msg in enumerate(messages)]
        response = queue.delete_messages(Entries=entries)
        print("show me delete message rsponse:", response)
        if 'Successful' in response:
            for msg_meta in response['Successful']:
                logger.info("Deleted %s", messages[int(
                    msg_meta['Id'])].receipt_handle)
        if 'Failed' in response:
            for msg_meta in response['Failed']:
                logger.warning(
                    "Could not delete %s",
                    messages[int(msg_meta['Id'])].receipt_handle
                )
    except ClientError:
        logger.exception("Couldn't delete messages from queue %s", queue)
    else:
        return response


def receive_messages(queue, maxNumber, waitTime):
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=maxNumber,
            WaitTimeSeconds=waitTime
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
            request = extract_request(msg.message_attributes)
            recommendations = get_recommendations(request)
            send_to_sns(request, recommendations)

    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages


def extract_request(message):
    request = {}
    request['cuisine'] = message.get('cuisine').get('StringValue')
    request['location'] = message.get('location').get('StringValue')
    request['number_of_people'] = message.get('number_of_people').get('StringValue')
    request['dining_time'] = message.get('dining_time').get('StringValue')
    request['phone_number'] = message.get('phone_number').get('StringValue')

    return request


def lambda_handler(event, context):
    
    messagesInQueue = True
    batchSize = 3
    while messagesInQueue:
        received_messages = receive_messages(queue, batchSize, 2)
        if received_messages:
            delete_messages(queue, received_messages)
        else:
            messagesInQueue = False

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully Execute Lambda2!')
    }