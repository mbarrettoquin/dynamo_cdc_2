import json
from datetime import datetime, timezone

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            new_image = record['dynamodb']['NewImage']
            
            if 'timestamp' in new_image:
                # The timestamp is expected to be in ISO 8601 format (e.g., '2023-08-20T12:00:00.000Z')
                # The 'Z' indicates UTC, which fromisoformat can parse in Python 3.11+
                # For broader compatibility, we can replace 'Z' with '+00:00'
                timestamp_str = new_image['timestamp']['S'].replace('Z', '+00:00')
                
                try:
                    # Parse the ISO 8601 timestamp from the DynamoDB record
                    write_time = datetime.fromisoformat(timestamp_str)
                    
                    # Get the current time in UTC
                    process_time = datetime.now(timezone.utc)
                    
                    # Calculate the latency
                    latency = process_time - write_time
                    
                    # Log the latency in milliseconds
                    latency_ms = latency.total_seconds() * 1000
                    print(f"Latency: {latency_ms:.2f} ms")
                    
                except (ValueError, KeyError) as e:
                    print(f"Error processing record: {e}")
                    print(f"Record data: {new_image}")

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
