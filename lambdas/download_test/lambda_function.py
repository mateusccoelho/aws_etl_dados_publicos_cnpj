import logging
import json

def lambda_handler(event, context):

    last_partition = int(sorted(event['partitionValues']['partitionValues'][0])[-1])
    last_update = event['LambdaResult']['ref_date']
    return {
        'statusCode': 200,
        'body': True if last_update > last_partition else False
    }
