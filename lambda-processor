import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    id_maquina = 1

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        results = json.loads(body.decode('utf-8'))
        
        for result in results:
            result['id'] = 0  # esto lo veremos la proxima clase
            result['id_maquina'] = 1 #esto también
            dynamodb.put_item(
                TableName='Respuestas', 
                Item={
                    'id': {'N': result['id']}, 
                    'id_maquina':{'N': result['id_maquina']},
                    'reference': {'S': result['Reference']},
                    'description': {'S': result["Description"].replace("/r", "")},
                    'state': {'N': result['STATE'] }
                    
                })
            
        

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
