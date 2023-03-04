import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """



def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def validate_dining_suggestion(location, cuisine, date, dining_time, number_of_people, phone_number):
    locations = ['manhattan', 'new york', 'nyc', 'new york city']
    if location is not None and location.lower() not in locations: 
        return build_validation_result(False, 
                                        'location', 
                                        'Sorry, our service is currently only available in Manhattan. Please choose a valid location.')
    
    
    cuisines = ["thai", "korean", "japanese", "american", "chinese"]
    if cuisine is not None and cuisine.lower() not in cuisines: 
        return build_validation_result(False, 
                                        'cuisine', 
                                        'Sorry, we don\'t have recommendations for {}.  Would you like to try something else? Our most popular cuisine is Korean.'.format(cuisine))

    if date is not None:
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'I did not understand that.  When would you like to eat?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'date', 'Please choose a date from today onward.  When would you like to eat?')

    if dining_time is not None:
        if len(dining_time) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', None)

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', None)
        

        if hour < 8 or hour > 24:
            # Outside of business hours
            return build_validation_result(False, 'dining_time', 'Our business hours are from 8 AM to 11 PM Can you specify a time during this range?')
    
   
    if number_of_people is not None:
        number = parse_int(number_of_people)
        if number < 1 or number > 50: 
            return build_validation_result(False, 'number_of_people', 'We accept up to 50 people.  Please enter a valid number between 1 and 50.')
    
    if phone_number is not None:
        if not phone_number.isnumeric() or len(phone_number) < 7 or len(phone_number) > 11:
            return build_validation_result(False,'phone_number','Please enter a valid United States phone number.')


    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """


def dining_suggestion(intent_request):
    """
    Performs dialog management and fulfillment for ordering flowers.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """

    location = get_slots(intent_request)['location']
    cuisine = get_slots(intent_request)['cuisine']
    date = get_slots(intent_request)['date']
    dining_time = get_slots(intent_request)['dining_time']
    number_of_people = get_slots(intent_request)['number_of_people']
    phone_number = get_slots(intent_request)['phone_number']
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_dining_suggestion(location, cuisine, date, dining_time, number_of_people, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        # Pass the price of the flowers back through session attributes to be used in various prompts defined
        # on the bot model.
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        return delegate(output_session_attributes, get_slots(intent_request))
    
    client = boto3.client('sqs')
    response = client.send_message(    
        QueueUrl='https://sqs.us-east-1.amazonaws.com/604242335225/messagequeue',
        MessageBody='msg from lf1',
        MessageAttributes={
            'location': {
                'StringValue' : get_slots(intent_request)['location'],
                'DataType': 'String'
            },
            'cuisine': {
                'StringValue' : get_slots(intent_request)['cuisine'], 
                'DataType' : 'String'
            },
            'date' : {
                'StringValue' : get_slots(intent_request)['date'],
                'DataType' : 'String'
            },
            'dining_time' : {
                'StringValue' : get_slots(intent_request)['dining_time'],
                'DataType' : 'String'
            },
            'number_of_people' : {
                'StringValue' : get_slots(intent_request)['number_of_people'], 
                'DataType' : 'String'
            },
            'phone_number' : {
                'StringValue' : get_slots(intent_request)['phone_number'],
                'DataType' : 'String'
            }
        }
    )   
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Generating results.  You will receive a text message with my recommendations shortly.  Have a nice day!'})


""" --- Intents --- """

def greeting(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hello! What can I do for you?'})

def thankYou(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You\'re very welcome!'})


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionIntent':
        return dining_suggestion(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thankYou(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    
    print("show me event:",event)
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)



