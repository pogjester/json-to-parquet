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
        # Process JSON data in chunks
        print(f"Loading JSON file in chunks: {input_file}")
        chunks = pd.read_json(input_file, chunksize=10000)  # Adjust chunksize as needed

        # Initialize an empty list to store DataFrame chunks
        df_list = []

        for chunk in chunks:
            df_list.append(chunk)

        # Concatenate all chunks into a single DataFrame
        df = pd.concat(df_list, ignore_index=True)

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
