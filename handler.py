import json
import boto3
import urllib.parse
import os
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

queue_url = sqs.get_queue_url(QueueName=os.getenv('FAILURE_QUEUE_NAME'))

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            bucket_name = record['s3']['bucket']['name']
            object_name = record['s3']['object']['key']
            object_size = record['s3']['object']['size']
            version_id = record['s3']['object']['versionId']

            # Key's come HTML encoded, we need to remove that. 
            object_name = urllib.parse.unquote_plus(object_name)
            lower_case_object_name = object_name.lower()
            if lower_case_object_name != object_name:
                print("Current object is not of the correct case (" + bucket_name + " / " + object_name +")")

                # If over ~ 5 GB copy with alternative method
                if object_size > 5368709100:
                    copy_large_object(bucket_name, object_name, lower_case_object_name)
                else:
                    copy_object(bucket_name, object_name, lower_case_object_name)
                delete_object_entirely(object_name, version_id, bucket_name)
            else:
                print("Already in the right case: (" + bucket_name + " / " + lower_case_object_name +")")
        except Exception as e:
            send_failed_object(record, e)
            raise
        
    return 

def copy_object(bucket_name, old_object_name, new_object_name):
    copy_source = {'Bucket': bucket_name, 'Key': old_object_name}
    s3.copy_object(CopySource = copy_source, Bucket = bucket_name, Key = new_object_name)

def copy_large_object(bucket_name, old_object_name, new_object_name):
    print("Large copy triggered")
    copy_source = {'Bucket': bucket_name, 'Key': old_object_name}
    s3.copy(CopySource = copy_source, Bucket = bucket_name, Key = new_object_name)

# Delete all previous versions of the object
def delete_object_entirely(object_name, version_id, bucket):
    s3.delete_object(Bucket=bucket, Key=object_name, VersionId=version_id)
    print(bucket + " " + object_name + " original deleted")

def send_failed_object(object, exception):
    error_message = {'ObjectInfo': object['s3'], 'ExceptionInfo': str(exception)}
    sqs.send_message(
        QueueUrl = queue_url['QueueUrl'],
        MessageBody = json.dumps(error_message)
    )
