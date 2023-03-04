import json
import boto3

client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    
    inputMessage = event['messages']
    botMessage = "Server is down, please come back later"
    
    if inputMessage is None or len(inputMessage) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    
    inputMessage = inputMessage[0]['unstructured']['text']
    # Update the user id, so it is different for different user
    response = client.post_text(
        botName='DiningConcierge',
        botAlias='$LATEST',
        userId='LF0',
        inputText=inputMessage)
    
    botResponse =  [{
        'type': 'unstructured',
        'unstructured': {
          'text': response['message']
        }
      }]
    return {
        'statusCode': 200,
        'messages': botResponse
    }