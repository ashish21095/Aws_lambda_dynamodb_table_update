def lambda_handler(event, context):
  # Get the details of the S3 event
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = event['Records'][0]['s3']['object']['key']

  # Download the file from S3
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, key)
  file_content = obj.get()['Body'].read().decode('utf-8')

  # Parse the CSV file
  reader = csv.DictReader(file_content.splitlines())

  # Iterate over the rows in the CSV file            
  for row in reader:
    # Check if the item already exists in the DynamoDB table
    response = dynamodb.get_item(
      TableName='activity_table',
      Key={
        'id': {'S': row['id']}
      }
    )
    item = response['Item']
    if item:
      # Update the existing item if it exists
      dynamodb.update_item(
        TableName='my_table',
        Key={
          'id': {'S': row['id']}
        },
        UpdateExpression='SET activity_name = :an',
        ExpressionAttributeValues={
          ':an': {'S': row['activity_name']}
        }
      )
    else:
      # Add the new item if it does not exist
      dynamodb.put_item(
        TableName='activity_table',
        Item={
          'id': {'S': row['id']},
          'activity_name': {'S': row['activity_name']}
        }
      )     
