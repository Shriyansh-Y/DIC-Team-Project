from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

table = dynamodb.Table('Tweets')
id = '12345'
keyword = 'lstest'
text = 'Test by Li'

table.put_item(Item={'id':id, 'keyword': keyword, 'text': text})


print(table.creation_date_time)