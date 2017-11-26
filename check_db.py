import boto3
from boto3.dynamodb.conditions import Attr


dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table("Tweets")
res = table.scan(FilterExpression=Attr('keyword').eq('blackfriday'))
items = res["Items"]
print(len(items))
