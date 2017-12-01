import boto3
from boto3.dynamodb.conditions import Attr


dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table("Tweets2")
#res = table.scan(FilterExpression=Attr('hashtags').contains('trump'))
res = table.scan()
items = res["Items"]
print(len(items))
