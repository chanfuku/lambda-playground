import json
import urllib.parse
import boto3
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler (event, context):
    # 1. DynamoDBのmailaddressテーブルを操作するオブジェクト
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('mailaddress')
    # 2. SQSのキューを操作するオブジェクト
    sqs = boto3.resource('sqs')
    queue= sqs.get_queue_by_name (QueueName='mailsendqueue0000001')
    for rec in event['Records']:
        # 3. S3に置かれたファイルパスを取得
        bucketname= rec['s3']['bucket']['name']
        filename= rec['s3']['object']['key']
        # 4. haserrorが0のものをmailaddressテーブルから取得
        response = table.query(
            IndexName='haserror-index',
            KeyConditionExpression=Key('haserror').eq(0)
        )
        # 5. 上記の1件1件についてループ処理
        for item in response['Items']:
            # 6. 送信済みを示すissendを0にする
            table.update_item(
                Key={'email': item['email']},
                UpdateExpression="set issend=:val",
                ExpressionAttributeValues= {
                    ':val': 0
                }
            )
            # 7. SQS にメッセージとして登録する
            sqsresponse = queue.send_message(
                MessageBody=item['email'],
                MessageAttributes={
                    'username': {
                        'DataType': 'String',
                        'StringValue': item['username']
                    },
                    'bucketname': {
                        'DataType': 'String',
                        'StringValue': bucketname
                    },
                    'filename': {
                        'DataType': 'String',
                        'StringValue': filename
                    }
                }
            )
            # 結果をログに出力しておく
            print(json.dumps(sqsresponse))

