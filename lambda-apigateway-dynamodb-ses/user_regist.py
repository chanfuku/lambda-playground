import json
import boto3
import base64
import time
import decimal
import os

# DynamoDBオブジェクト
dynamodb = boto3.resource('dynamodb')
# S3オブジェクト
s3  = boto3.client('s3')
# SESオブジェクト
ses = boto3.client('ses')

# メール送信関数
def sendmail(to, subject, body):
  mailFrom = os.environ['MAILFROM']
  response = ses.send_email(
    Source = mailFrom,
    ReplyToAddresses = [mailFrom],
    Destination = {
      'ToAddresses' : [to]
    },
    Message = {
      'Subject' : {
        'Data': subject,
        'Charset': 'UTF-8'
      },
      'Body' : {
        'Text' : {
          'Data': body,
          'Charset': 'UTF-8'
        }
      }
    }
  )

# 連番を更新して返す関数
def next_seq(table, tablename):
  response = table.update_item(
    Key={
      'tablename' : tablename
    },
    UpdateExpression="set seq = seq + :val",
    ExpressionAttributeValues={
      ':val': 1
    },
    ReturnValues='UPDATED_NEW'
  )
  return response['Attributes']['seq']

def lambda_handler(event, context):
  # OPTIONメソッドの時は何もしない
  operation = event['httpMethod']
  if operation == 'OPTIONS':
    return {
      'statusCode' : 204,
      'headers': {
        'content-type': 'application/json'
      },
      'body' : json.dumps({
        'message': ''
      })
    }

  try:
    # シーケンスデータを得る
    seqtable = dynamodb.Table('sequence')
    nextseq = next_seq(seqtable, 'user')
    # フォームに入力されたデータを得る
    body = event['body']
    if event['isBase64Encoded']:
      body = base64.b64decode(body)

    decoded = json.loads(body)
    username = decoded['username']
    email = decoded['email']
    inquiry = decoded['inquiry']
    # クライアントのIPアドレスを得る
    host = event['requestContext']['identity']['sourceIp']

    # 署名付きURLを作る
    url = s3.generate_presigned_url(
      ClientMethod = 'get_object',
      Params = {
        'Bucket' : os.environ['SAVED_BUCKET'],
        'Key' : os.environ['FILE_NAME']
      },
      # 一週間
      ExpiresIn = 24 * 60 * 60 * 7,
      HttpMethod = 'GET')

    # 現在のUNIXタイムスタンプを得る
    now = time.time()
    # userテーブルに登録する
    usertable = dynamodb.Table('user')
    usertable.put_item(
      Item={
        'id' : nextseq,
        'username' : username,
        'email' : email,
        'inquiry': inquiry,
        'accepted_at' : decimal.Decimal(str(now)),
        'host' : host,
        'url' : url
      }
    )

    # メールを送信する
    mailbody = """
{0}様
お問い合わせいただきありがとうございます。
近日中に担当者よりご連絡させていただきます。
資料は下記のURLからダウンロードできます。※1週間で有効期限が切れますのでご注意ください。
{1}
""".format(username, url)
    sendmail(email, "お問い合わせいただきありがとうございます", mailbody)

    # 結果を返す
    return {
      'statusCode' : 200,
      'headers': {
        'content-type': 'application/json'
      },
      'body' : json.dumps({
        'message': '成功'
      })
    }

  except:
    # エラーメッセージを返す
    import traceback
    err = traceback.format_exc()
    print(err)
    return {
      'statusCode' : 500,
      'headers': {
        'content-type': 'application/json'
      },
      'body' : json.dumps({
        'error': '内部エラーが発生しました'
      })
    }
