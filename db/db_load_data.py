from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

table = dynamodb.Table('Tweets')
#id = '12345'
#hashtags = ['lstest', 'lishi']
#text = 'Test by Li'

#table.put_item(Item={'id':id, 'hashtags': hashtags, 'text': text})

table.put_item(Item={'id':str(100), 'hashtags': ['testbylishi'], 'text': 'test01'})

print(table.creation_date_time)