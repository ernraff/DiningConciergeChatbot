import decimal
import json
import logging
import sys
import boto3
from botocore.exceptions import ClientError
import requests
from boto3.dynamodb.conditions import Key
import botocore
from requests_aws4auth import AWS4Auth



region = 'us-east-1'
logger = logging.getLogger(__name__)
sqs = boto3.resource('sqs',region)

dynamodb = boto3.resource('dynamodb', region)
table = dynamodb.Table("yelp-restaurants")


service = 'es'
index = 'restaurants'

host = 'https://search-restaurants-wchgwotnvkcsqoff7wthugxfqq.us-east-1.es.amazonaws.com'
index = 'restaurants'
datatype = 'Restaurant'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


# print("show me credential:", credentials)

# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

url = host + '/' + index + '/_search?'

headers = headers = {
    'Content-Type': "application/json",
    'Accept': "/",
}

requiredData = ["id", 'cuisine']

sns = boto3.client('sns', region)


def create_topic(name):
    """
    Creates a notification topic.

    :param name: The name of the topic to create.
    :return: The newly created topic.
    """
    sns = boto3.resource("sns")
    topic = sns.create_topic(Name=name)
    return topic


def get_restaurant(id):

    # print("getrestaurant id:", id)

    try:
        response = table.query(
            KeyConditionExpression=Key('id').eq(id)
        )
    except:
        print("no response from DB")
        return None
    else:
        return response["Items"]


def get_queue(name):
    """
    Gets an SQS queue by name.
    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    print("it is here to get queue")
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
 # type: ignore        print("it is getting Queue now!")
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        print("got the queue here")
        return queue


def receive_messages(queue, max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        for msg in messages:
            print("Received message: %s: %s", msg.message_id, msg.body)
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        print("Couldn't receive messages from queue: %s", queue)
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        print("taking all the messages!")
        return messages


def delete_messages(queue, messages):
    """
    Delete a batch of messages from a queue in a single request.
    Usage is shown in usage_demo at the end of this module.
    :param queue: The queue from which to delete the messages.
    :param messages: The list of messages to delete.
    :return: The response from SQS that contains the list of successful and failed
             message deletions.
    """
    try:
        entries = [{
            'Id': str(ind),
            'ReceiptHandle': msg.receipt_handle
        } for ind, msg in enumerate(messages)]
        response = queue.delete_messages(Entries=entries)
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


def unpack_message(msg):
    return (msg.body, msg.message_attributes)


def unpack_source(source):

    sourceAttribute = []

    for key in requiredData:
        try:
            source[key]
        except:
            sourceAttribute.append(None)
        else:
            sourceAttribute.append(source[key])

    sourceId = sourceAttribute[0]
    sourceCuisine = sourceAttribute[1]
    return (sourceId, sourceCuisine)


def DataSync(messageResponse):
    # where we can extract cuisine that a user wants
    request = {}
    request.update(cuisine=messageResponse["cuisine"]['StringValue'])
    request.update(date=messageResponse["date"]['StringValue'])
    request.update(dining_time=messageResponse["dining_time"]['StringValue'])
    request.update(
        number_of_people=messageResponse["number_of_people"]['StringValue'])
    request.update(phone_number=messageResponse["phone_number"]['StringValue'])

    search_params = {
        "from": 0,
        "size": 5,
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "cuisine": request['cuisine']
                    }
                },
                "random_score": {}
            }
        }
    }

    # extract elastic search
    jsonhResponse = requests.get(url, auth=awsauth, data=json.dumps(search_params), headers=headers)
    print("show jsonResponse",jsonhResponse, '\n')
    elasticSearchResponse = json.loads(jsonhResponse.text)
    sourceList = elasticSearchResponse["hits"]["hits"]
    restaurantList = []

    for eachRestaurant in sourceList:
        eachSource = eachRestaurant["_source"]

        id, cuisine = unpack_source(eachSource)
        if (id == None or cuisine == None):
            # data validation fails so we do not collect it
            continue
        else:
            if request['cuisine'] == cuisine:
                # print("show me id:{} and cuisine:{}".format(id, cuisine))
                # we query the restaurants from DB
                response = get_restaurant(id)
                if response == None:
                    return None
                restaurantList.append(response)

    return restaurantList, request


client = boto3.client('ses', region_name=region)
CHARSET = "UTF-8"


def send_to_sns(userData, restaurantData):
    SENDER = "eor2014@nyu.edu"
    RECIPIENT = "eor2014@nyu.edu"

    BODY_TEXT = "this is restaurant suggestions for {}. \n".format(
        userData["cuisine"])

    for eachRestaurant in restaurantData:
        if (eachRestaurant == []):
            continue
        eachRestaurantInfo = eachRestaurant[0]
        subText = "{} , phone number {}, at {}, {} with rating {} \n".format(eachRestaurantInfo["name"], "No Phone Number" if eachRestaurantInfo[
                                                                             "phone"] == None else eachRestaurantInfo["phone"], eachRestaurantInfo["address1"], eachRestaurantInfo["zip_code"], eachRestaurantInfo["rating"])
        BODY_TEXT = BODY_TEXT + (subText)

    SUBJECT = "All the list of {} restaurants!!".format(userData["cuisine"])

    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def lambda_handler(event, context):
    # TODO implement

    # call queue first
    receievedQueue = get_queue("messagequeue")

    more_messages = True

    while more_messages:
        received_messages = receive_messages(receievedQueue, 2, 2)
        print('.', end='')
        sys.stdout.flush()
        for message in received_messages:
            body, attributes = unpack_message(message)
            messageResponse = attributes
            restaurantList = None
            userData = {}
            while (True):
                print("keep query DB data...")
                restaurantList, userData = DataSync(messageResponse)
                if restaurantList != None:
                    break
            # print("show me restaurant List:", restaurantList)
            send_to_sns(userData, restaurantList)

        if received_messages:
            delete_messages(receievedQueue, received_messages)
        else:
            more_messages = False

    print('Done.')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


lambda_handler(None, None)
