from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

table = dynamodb.Table('Movies')
year=int(1990)
title = 'Batman'
info = 'The one with the dark knight'

table.put_item(Item={'year':year, 'title': title, 'info': info})


