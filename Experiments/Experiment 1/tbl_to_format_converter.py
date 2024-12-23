import pandas as pd
import os
import argparse

# Define a dictionary mapping each .tbl file to its respective columns
tbl_columns = {
    "customer.tbl": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
    "orders.tbl": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
    "lineitem.tbl": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax",
                    "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
    "part.tbl": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
    "partsupp.tbl": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    "supplier.tbl": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
    "nation.tbl": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "region.tbl": ["r_regionkey", "r_name", "r_comment"]
}

def convert_tbl_to_csv_and_parquet(tbl_file_path, output_format):
    tbl_file_name = os.path.basename(tbl_file_path)
    column_names = tbl_columns.get(tbl_file_name)

    if column_names is None:
        print(f"Column names not found for {tbl_file_name}. Skipping file.")
        return

    # Set chunk size (adjust based on available RAM and file size)
    chunksize = 1000000  # 1 million rows per chunk

    # Output file paths
    csv_file_path = os.path.splitext(tbl_file_path)[0] + '.csv'
    parquet_file_path = os.path.splitext(tbl_file_path)[0] + '.parquet'

    print(f"Converting {tbl_file_name} to {output_format}...")

    # Initialize an empty list to store chunks for Parquet
    parquet_chunks = []

    # If CSV output is requested and file exists, remove it to avoid appending to old data
    if output_format in ('csv', 'both') and os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # Process the .tbl file in chunks
    try:
        with pd.read_csv(tbl_file_path, sep='|', header=None, names=column_names,
                         engine='python', index_col=False, chunksize=chunksize) as reader:
            for i, chunk in enumerate(reader):
                # Remove the last column (empty due to trailing | in .tbl files)
                chunk = chunk.iloc[:, :-1]

                # If CSV output is requested, save chunk to CSV
                if output_format in ('csv', 'both'):
                    chunk.to_csv(csv_file_path, mode='a', header=(i == 0), index=False)

                # If Parquet output is requested, store the chunk in a list
                if output_format in ('parquet', 'both'):
                    parquet_chunks.append(chunk)

                print(f"Processed chunk {i + 1} for {tbl_file_name}")

        # If Parquet output is requested, concatenate chunks and save to a single Parquet file
        if output_format in ('parquet', 'both') and parquet_chunks:
            all_parquet_data = pd.concat(parquet_chunks, ignore_index=True)
            all_parquet_data.to_parquet(parquet_file_path, index=False)
            print(f"Parquet file saved to {parquet_file_path}")

        if output_format in ('csv', 'both'):
            print(f"CSV file saved to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred while processing {tbl_file_name}: {e}")

    print(f"Finished processing {tbl_file_name}\n")

def process_all_files(directory_path, output_format):
    # Ensure the directory exists
    if not os.path.isdir(directory_path):
        print(f"The directory {directory_path} does not exist.")
        return

    # List all files in the directory
    tbl_files = [file for file in os.listdir(directory_path) if file.endswith('.tbl')]

    if not tbl_files:
        print(f"No .tbl files found in the directory {directory_path}.")
        return

    # Process each .tbl file
    for tbl_file in tbl_files:
        tbl_file_path = os.path.join(directory_path, tbl_file)
        convert_tbl_to_csv_and_parquet(tbl_file_path, output_format)

if __name__ == "__main__":
    # Create argument parser
    parser = argparse.ArgumentParser(description='Convert all .tbl files in a directory to CSV and/or Parquet formats.')
    parser.add_argument('directory', type=str, help='Path to the directory containing .tbl files')
    parser.add_argument('format', choices=['csv', 'parquet', 'both'], help='Output format: csv, parquet, or both')

    # Parse command-line arguments
    args = parser.parse_args()

    # Process all .tbl files in the specified directory
    process_all_files(args.directory, args.format)
