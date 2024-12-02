import pandas as pd
import argparse

def convert_json_to_parquet(input_file, output_file):
    """
    Converts a JSON file to a Parquet file.

    Args:
        input_file (str): Path to the input JSON file.
        output_file (str): Path to the output Parquet file.
    """
    try:
        # Load JSON data into a DataFrame
        print(f"Loading JSON file: {input_file}")
        df = pd.read_json(input_file, lines=True)  # Use `lines=True` for newline-delimited JSON

        # Convert DataFrame to Parquet
        print(f"Converting to Parquet file: {output_file}")
        df.to_parquet(output_file, engine='pyarrow', compression='snappy')
        print("Conversion completed successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Convert JSON to Parquet")
    parser.add_argument("input_file", help="Path to the input JSON file")
    parser.add_argument("output_file", help="Path to the output Parquet file")
    args = parser.parse_args()

    # Run the conversion
    convert_json_to_parquet(args.input_file, args.output_file)
