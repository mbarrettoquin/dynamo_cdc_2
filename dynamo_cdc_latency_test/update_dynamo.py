import boto3
import uuid
from datetime import datetime, timezone
import time
import argparse

def update_dynamo_table(table_name, region, num_items, delay):
    """
    Writes a specified number of items to a DynamoDB table.

    :param table_name: The name of the DynamoDB table.
    :param region: The AWS region of the table.
    :param num_items: The number of items to write to the table.
    :param delay: The delay in seconds between writes.
    """
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    for i in range(num_items):
        item_id = str(uuid.uuid4())
        current_time_iso = datetime.now(timezone.utc).isoformat()

        item = {
            'id': item_id,
            'timestamp': current_time_iso,
            'message': f'Test message {i+1}'
        }

        try:
            table.put_item(Item=item)
            print(f"Wrote item {item_id} to {table_name} at {current_time_iso}")
        except Exception as e:
            print(f"Error writing to DynamoDB: {e}")
            break # Exit if there's an error

        if delay > 0:
            time.sleep(delay)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write items to a DynamoDB table to test CDC latency.")
    parser.add_argument("--table-name", required=True, help="The name of the DynamoDB table.")
    parser.add_argument("--region", required=True, help="The AWS region of the DynamoDB table.")
    parser.add_argument("--num-items", type=int, default=10, help="The number of items to write.")
    parser.add_argument("--delay", type=float, default=1.0, help="The delay in seconds between writes.")
    
    args = parser.parse_args()

    update_dynamo_table(args.table_name, args.region, args.num_items, args.delay)
