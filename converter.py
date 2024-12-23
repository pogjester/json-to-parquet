import ijson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
from pandas import json_normalize
import os

def convert_json_to_parquet_streaming(input_file, output_file_prefix):
    """
    Converts a massive nested JSON file to multiple Parquet files using streaming.

    Args:
        input_file (str): Path to the input JSON file.
        output_file_prefix (str): Prefix for the output Parquet files.
    """
    try:
        print(f"Opening input file: {input_file}")
        # Open the input JSON file
        with open(input_file, 'r') as f:
            # Initialize a list to store JSON objects
            records = []
            record_count = 0
            batch_count = 0

            print("Starting to parse 'reporting_structure' array...")
            # Use ijson to parse the "reporting_structure" array incrementally
            for record in ijson.items(f, 'reporting_structure.item'):
                print(f"Parsing record {record_count + 1}...")
                # Flatten the nested JSON structure
                try:
                    flattened_record = json_normalize(record)
                    records.append(flattened_record)
                except Exception as e:
                    print(f"Error flattening record {record_count + 1}: {e}")

                record_count += 1

                # Process in batches to avoid memory issues
                if len(records) >= 10000:  # Adjust batch size as needed
                    batch_count += 1
                    print(f"Processing batch {batch_count} with {len(records)} records...")
                    try:
                        df = pd.concat(records, ignore_index=True)  # Combine all flattened records
                        print(f"Batch {batch_count}: DataFrame created with shape {df.shape}.")
                        table = pa.Table.from_pandas(df)
                        print(f"Batch {batch_count}: Parquet table created.")
                        # Write each batch to a separate Parquet file
                        batch_file = f"{output_file_prefix}_batch_{batch_count}.parquet"
                        pq.write_table(table, batch_file, compression='snappy')
                        print(f"Batch {batch_count}: Written to Parquet file: {batch_file}.")
                    except Exception as e:
                        print(f"Error processing batch {batch_count}: {e}")
                    finally:
                        records = []  # Clear the list for the next batch

            # Write any remaining records
            if records:
                batch_count += 1
                print(f"Processing final batch with {len(records)} records...")
                try:
                    df = pd.concat(records, ignore_index=True)  # Combine all flattened records
                    print(f"Final batch: DataFrame created with shape {df.shape}.")
                    table = pa.Table.from_pandas(df)
                    print(f"Final batch: Parquet table created.")
                    batch_file = f"{output_file_prefix}_batch_{batch_count}.parquet"
                    pq.write_table(table, batch_file, compression='snappy')
                    print(f"Final batch: Written to Parquet file: {batch_file}.")
                except Exception as e:
                    print(f"Error processing final batch: {e}")

        print(f"Conversion completed successfully! Total records processed: {record_count}. Total batches: {batch_count}.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Convert JSON to Parquet using streaming")
    parser.add_argument("input_file", help="Path to the input JSON file")
    parser.add_argument("output_file_prefix", help="Prefix for the output Parquet files")
    args = parser.parse_args()

    # Run the conversion
    convert_json_to_parquet_streaming(args.input_file, args.output_file_prefix)
