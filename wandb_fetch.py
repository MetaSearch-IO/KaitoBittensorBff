import wandb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def fetch_run_data(api, run_id, start_time=None):
    """
    Fetch metrics dataframe for a specific run_id, filtered by start_time.
    Early exit if data is sorted in descending order by timestamp.
    """
    run = api.run(f"openkaito/sn5-validators/{run_id}")
    
    # Scan the history and apply filtering by start_time
    history_data = []
    for row in run.scan_history():
        timestamp = row.get('_timestamp', None)  # Get the '_timestamp' field

        # Apply timestamp filtering if provided
        if timestamp is not None:
            if start_time is not None and timestamp < start_time:
                break  # Early exit if we encounter a timestamp earlier than start_time
            # Append the row if it passes the timestamp filter
            history_data.append(row)

    # Convert the filtered data to a pandas DataFrame
    metrics_dataframe = pd.DataFrame(history_data)
    return metrics_dataframe, run_id

if __name__ == "__main__":
    # Set the timeout value
    api = wandb.Api(timeout=29)

    # Fetch all runs for the specified project
    runs = api.runs("openkaito/sn5-validators")

    # Extract run IDs
    run_ids = [run.id for run in runs]

    print("Run IDs:", run_ids)

    # Define the start_time for filtering (None means no filtering)
    start_time = datetime(2023, 9, 20).timestamp()  # Set start time (UNIX timestamp)

    # List to store all metrics dataframes
    all_metrics = []

    # Use ThreadPoolExecutor to fetch run data in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks, passing start_time for filtering
        futures = [executor.submit(fetch_run_data, api, run_id, start_time) for run_id in run_ids]

        # Process the results of concurrent execution
        for future in as_completed(futures):
            try:
                # Get the result from the future, which includes both metrics and run_id
                metrics_dataframe, run_id = future.result()
                print(f"Metrics for run {run_id}:\n", metrics_dataframe)

                # Append each dataframe to the list
                all_metrics.append(metrics_dataframe)

            except Exception as e:
                # Handle any error during the fetching process
                print(f"Error fetching data for run: {e}")

    # Merge all dataframes
    if all_metrics:
        combined_df = pd.concat(all_metrics, ignore_index=True)

        # Print the combined dataframe
        print("Combined DataFrame:\n", combined_df)

        # Filter the data (example: keep only 'loss' and 'accuracy' columns)
        filtered_df = combined_df[['loss', 'accuracy']]  # Adjust filter based on your needs

        # Group by (example: group by 'epoch' and compute the mean for each group)
        grouped_df = filtered_df.groupby('epoch').mean().reset_index()

        # Print the grouped and aggregated dataframe
        print("Grouped and Aggregated DataFrame:\n", grouped_df)

        # Save the final grouped dataframe to a CSV file
        grouped_df.to_csv("grouped_metrics.csv", index=False)
    else:
        print("No data was fetched.")