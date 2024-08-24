import json
import boto3
import time
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    print(event)
    for i in event['Records']:
        ddb = i['dynamodb']
        resturant_id = (ddb['Keys']['resturant_id_user_id']['S']).split('#')[0]
        print("The resturant_id is : ",resturant_id)
        response = client.query(
            TableName = "aggregatedfavourite",
            KeyConditionExpression = "resturant_id =:resturant_id",
            ExpressionAttributeValues = {
                ":resturant_id":{"S":resturant_id}
            },
            ScanIndexForward = False, Limit = 1
        )

        if (response['Count'] == 0):
            print("The resturant is becoming favorite for First time")
            response = client.put_item(TableName = "aggregatedfavourite", Item = {"resturant_id":{"S":resturant_id},"favourite_count":{'N':'1'}})
        else:
            previous_counter = int(response['Items'][0]['favourite_count']['N'])
            if (i['eventName'] == 'REMOVE'):
                new_counter = previous_counter-1
            else:
                new_counter = previous_counter+1  
            response = client.update_item(
                TableName = "aggregatedfavourite",
                key = {
                    "resturant_id":{'S':f'{resturant_id}'}
                },
                UpdateExpression = 'SET favourite_count =:favourite_count',
                ExpressionAttributeValues = {
                    ':favourite_count':{'N':f'{new_counter}'}
                }
            )
    return {
        'statusCode':200,
        'body':json.dumps("Hello from Lambda")
    }              
