import ijson
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def convert_json_to_parquet_streaming(input_file, output_file):
    """
    Converts a large JSON file to a Parquet file using streaming.

    Args:
        input_file (str): Path to the input JSON file.
        output_file (str): Path to the output Parquet file.
    """
    try:
        # Open the input JSON file
        with open(input_file, 'r') as f:
            # Initialize a list to store JSON objects
            records = []

            # Use ijson to parse the JSON file incrementally
            for record in ijson.items(f, 'item'):
                records.append(record)

                # Process in batches to avoid memory issues
                if len(records) >= 10000:  # Adjust batch size as needed
                    df = pd.DataFrame(records)
                    table = pa.Table.from_pandas(df)
                    # Write to a single Parquet file
                    print(f"Written {len(records)}")
                    pq.write_table(table, output_file, compression='snappy', append=True)
                    records = []  # Clear the list for the next batch

            # Write any remaining records
            if records:
                df = pd.DataFrame(records)
                table = pa.Table.from_pandas(df)
                print("Writing dataframe to parquet...")
                pq.write_table(table, output_file, compression='snappy', append=True)

        print("Conversion completed successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Convert JSON to Parquet using streaming")
    parser.add_argument("input_file", help="Path to the input JSON file")
    parser.add_argument("output_file", help="Path to the output Parquet file")
    args = parser.parse_args()

    # Run the conversion
    convert_json_to_parquet_streaming(args.input_file, args.output_file)