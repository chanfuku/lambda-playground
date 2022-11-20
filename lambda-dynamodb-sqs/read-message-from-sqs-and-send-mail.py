import json
import boto3

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('mailaddress')
client = boto3.client('ses')
MAILFROM = 'SESでverifiedされているメールアドレスに変更してください' # メールアドレスの変更

def lambda_handler (event, context):
    print(event)
    for rec in event['Records']:
        email = rec['body']
        bucketname = rec['messageAttributes']['bucketname']['stringValue']
        filename = rec ['messageAttributes']['filename']['stringValue']
        username = rec['messageAttributes']['username']['stringValue']

        #S3バケットから本文を取得する
        obj = s3.Object(bucketname, filename)
        response = obj.get()
        maildata = response['Body'].read().decode('utf-8')
        data = maildata.split("\n", 3)
        subject = data[0]
        body= data[2]
        # 送信済みでないことを確認し、また、送信済みに設定する
        response = table.update_item(
            Key = {
                'email': email
            },
            UpdateExpression = "set issend=:val",
            ExpressionAttributeValues = {
                ':val': 1
            },
            ReturnValues = 'UPDATED_OLD'
        )
        #未送信なら送信
        if response['Attributes']['issend'] == 0:
            #メール送信
            response = client.send_email(
                Source=MAILFROM,
                ReplyToAddresses=[MAILFROM],
                Destination= {
                    'ToAddresses': [
                        email
                    ]
                },
                Message={
                    'Subject': {
                        'Data': subject,
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Text': {
                            'Data': body,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )

        else:
            print("Resend Skip")
