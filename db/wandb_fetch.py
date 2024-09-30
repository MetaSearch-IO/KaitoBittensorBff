import wandb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import boto3
import json
import re  # Import regular expressions for extracting miner_id

# Define function to send records to Kinesis stream with retries in case of failure
def put_records_to_kinesis(records_to_kinesis, kinesis_client, stream_name: str, retry_times: int):
    failed_records = []
    codes = []
    err_msg = ""

    response = None
    try:
        # Attempt to send records to Kinesis
        response = kinesis_client.put_records(
            Records=records_to_kinesis,
            StreamName=stream_name,
        )
    except Exception as e:
        # If an error occurs, store the failed records and error message
        failed_records = records_to_kinesis
        err_msg = str(e)

    # If no general failure, check if there are specific failed records
    if not failed_records and response and response["FailedRecordCount"] > 0:
        for idx, res in enumerate(response["Records"]):
            # Skip successful records
            if not res.get("ErrorCode"):
                continue
            # Store error codes and corresponding failed records
            codes.append(res["ErrorCode"])
            failed_records.append(records_to_kinesis[idx])

        err_msg = "Individual error codes: " + ",".join(codes)

    # Retry logic for failed records
    if failed_records:
        if retry_times > 0:
            print(
                "Some records failed while calling PutRecords to Kinesis stream, retrying. %s"
                % (err_msg)
            )
            time.sleep(1)
            # Recursive retry
            put_records_to_kinesis(
                failed_records, kinesis_client, stream_name, retry_times - 1
            )
        else:
            raise RuntimeError(
                "Could not put records after %s attempts. %s"
                % (str(retry_times), err_msg)
            )

# Function to extract miner_id from keys and fetch relevant metrics
def extract_miner_id(key):
    """
    Extract miner_id from a key like 'TextEmbeddingSynapse_raw_scores.158'.
    The miner_id is the number after the last period in the key.
    """
    match = re.search(r'\.(\d+)$', key)
    if match:
        return int(match.group(1))
    return None

# Function to fetch data for a specific run_id from wandb, focusing on relevant metrics
def fetch_run_data(api, run_id, start_time=None):
    """
    Fetch metrics dataframe for a specific run_id, filtered by start_time and required metrics.
    """
    print(f'Fetching data for run: {run_id}')
    run = api.run(f"openkaito/sn5-validators/{run_id}")
    
    # Scan the run's history and filter by start_time if provided
    history_data = []
    for row in run.scan_history(page_size=1): 
        timestamp = row.get('_timestamp', None)

        if timestamp is not None:
            if start_time is not None and timestamp < start_time:
                break

            # Initialize a dictionary to store the row data
            filtered_row = {
                "run_name": run_id,
                "miner_id": None,
                "timestamp": timestamp,
                "score": None,
                "loss": None,
                "top1_recall": None,
                "top3_recall": None
            }

            # Iterate over the row's keys to find the relevant metrics
            for key, value in row.items():
                if key.startswith("TextEmbeddingSynapse_raw_scores") and value is not None:
                    filtered_row["score"] = value
                    filtered_row["miner_id"] = extract_miner_id(key)
                elif key.startswith("TextEmbeddingSynapse_losses") and value is not None:
                    filtered_row["loss"] = value
                    filtered_row["miner_id"] = extract_miner_id(key)
                elif key.startswith("TextEmbeddingSynapse_top1_recalls") and value is not None:
                    filtered_row["top1_recall"] = value
                    filtered_row["miner_id"] = extract_miner_id(key)
                elif key.startswith("TextEmbeddingSynapse_top3_recalls") and value is not None:
                    filtered_row["top3_recall"] = value
                    filtered_row["miner_id"] = extract_miner_id(key)

            # Only append the row if all required fields (score, loss, etc.) have been populated
            if all(filtered_row[field] is not None for field in ["score", "loss", "top1_recall", "top3_recall"]):
                history_data.append(filtered_row)

    # Convert data to a Pandas DataFrame
    metrics_dataframe = pd.DataFrame(history_data)
    
    return metrics_dataframe, run_id

# Function to send the filtered wandb data to Kinesis
def send_to_kinesis(metrics_dataframe, stream_name, kinesis_client):
    records_to_kinesis = []
    for index, row in metrics_dataframe.iterrows():
        # Create a record for Kinesis with data and partition key
        record = {
            "Data": json.dumps(row.to_dict()) + "\n",
            "PartitionKey": str(hash(row["run_name"]))
        }
        records_to_kinesis.append(record)

    # Send records to Kinesis using the put_records_to_kinesis function
    put_records_to_kinesis(records_to_kinesis, kinesis_client, stream_name, 10)

if __name__ == "__main__":
    # Set up Kinesis client using boto3
    kinesis_client = boto3.client("kinesis", region_name="us-west-2")
    stream_name = "your-kinesis-stream-name"

    # Set up WandB API
    api = wandb.Api(timeout=180)

    # Define the run IDs you want to process
    run_ids = ['mj4f45m1']
    start_time = datetime(2023, 9, 20).timestamp()  # Filter start time

    all_metrics = []

    # Use ThreadPoolExecutor to fetch multiple run data in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_run_data, api, run_id, start_time) for run_id in run_ids]

        for future in as_completed(futures):
            try:
                # Process the fetched data for each run
                metrics_dataframe, run_id = future.result()
                print(f"Metrics for run {run_id}:\n", metrics_dataframe)

                # Send the fetched data to Kinesis
                send_to_kinesis(metrics_dataframe, stream_name, kinesis_client)

            except Exception as e:
                print(f"Error fetching data for run: {e}")

    print("Data has been successfully sent to Kinesis.")