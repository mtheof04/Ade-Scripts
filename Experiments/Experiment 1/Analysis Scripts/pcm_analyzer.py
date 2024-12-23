import os
import sys
import re
import pandas as pd
from pathlib import Path

def find_pcm_stats_files(base_path):
    """
    Recursively find all pcm_stats.txt files under base_path.
    """
    pcm_stats_files = []
    base = Path(base_path)
    print(f"Starting recursive search for pcm_stats.txt files under: {base_path}\n")
    
    # Traverse all directories under base_path
    for root, dirs, files in os.walk(base):
        for file in files:
            if 'pcm_stats' in file.lower() and file.lower().endswith('.txt'):
                file_path = Path(root) / file
                pcm_stats_files.append(str(file_path))
                print(f"  Found pcm_stats.txt: {file_path}")
    
    print(f"\nTotal pcm_stats.txt files found: {len(pcm_stats_files)}\n")
    return pcm_stats_files

def process_pcm_stats_file(file_path):
    """
    Process a single pcm_stats.txt file and extract all 'System Memory Throughput(MB/s)' values.
    Returns a list of throughput values as floats.
    """
    throughputs = []
    
    if not os.path.exists(file_path):
        print(f"Warning: File not found - {file_path}")
        return throughputs
    
    print(f"Reading file: {file_path}")
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return throughputs
    
    # Define regex pattern for 'System Memory Throughput(MB/s): <value>'
    pattern = re.compile(r"System Memory Throughput\s*\(MB/s\):\s*([\d.]+)", re.IGNORECASE)
    
    for line in lines:
        match = pattern.search(line)
        if match:
            throughput_value = float(match.group(1))
            throughputs.append(throughput_value)
            print(f"  Extracted System Memory Throughput(MB/s): {throughput_value} MB/s")
    
    if not throughputs:
        print(f"  No 'System Memory Throughput(MB/s)' data found in {file_path}")

        print("  Sample lines from the file:")
        sample_lines = lines[:10]  # First 10 lines for context
        for sample_line in sample_lines:
            print(f"    {sample_line.strip()}")
    
    return throughputs

def aggregate_throughput(pcm_stats_files):
    """
    Aggregate 'System Memory Throughput(MB/s)' across multiple pcm_stats.txt files.
    Returns a nested dictionary mapping SF*, F*, Data Type (Csv/Parquet) to their throughput values list, average, and max.
    """
    sf_data = {}
    
    for file_path in pcm_stats_files:
        parts = Path(file_path).parts
        try:
            sf_dir = parts[-5]       # SF*
            f_dir = parts[-4]        # F* (e.g., F1.0)
            data_type = parts[-3]    # Csv or Parquet
            iteration_dir = parts[-2]  # Iteration*
        except IndexError:
            print(f"Warning: Unexpected directory structure for '{file_path}'")
            continue
        
        # Initialize nested dictionaries
        if sf_dir not in sf_data:
            sf_data[sf_dir] = {}
        if f_dir not in sf_data[sf_dir]:
            sf_data[sf_dir][f_dir] = {}
        if data_type not in sf_data[sf_dir][f_dir]:
            sf_data[sf_dir][f_dir][data_type] = {
                'throughputs': []
            }
        
        # Process the pcm_stats.txt file
        throughputs = process_pcm_stats_file(file_path)
        
        if throughputs:
            sf_data[sf_dir][f_dir][data_type]['throughputs'].extend(throughputs)
        else:
            print(f"  Skipping file due to missing data: {file_path}")
    
    # Compute averages and max values, and prepare data for printing
    sf_averages = {}
    for sf, f_dirs in sf_data.items():
        sf_averages[sf] = {}
        for f_dir, data_types in f_dirs.items():
            if f_dir not in sf_averages[sf]:
                sf_averages[sf][f_dir] = {}
            for data_type, data in data_types.items():
                throughputs = data['throughputs']
                if throughputs:
                    avg_throughput = sum(throughputs) / len(throughputs)
                    max_throughput = max(throughputs)
                    sf_averages[sf][f_dir][data_type] = {
                        'throughputs': throughputs,
                        'average': avg_throughput,
                        'max': max_throughput
                    }
                    print(f"  Averaged {data_type} for {sf}/{f_dir}: {avg_throughput:.2f} MB/s over {len(throughputs)} iterations")
                else:
                    sf_averages[sf][f_dir][data_type] = {
                        'throughputs': [],
                        'average': None,  # or set to 0
                        'max': None
                    }
                    print(f"  No throughput data to average for {data_type} in {sf}/{f_dir}")
    
    return sf_averages

def save_to_excel(base_path, sf_averages):
    """
    Save the aggregated throughput data to Excel files.
    Each SF* will have its own Excel file with separate sheets for each F* and Data Type (Csv/Parquet).
    Each sheet contains 'Average' and 'Max System Memory Throughput (MB/s)'.
    """
    for sf, f_dirs in sf_averages.items():
        excel_file_name = f"memory_bandwidth_{sf}_combined.xlsx"
        excel_file_path = Path(base_path) / sf / excel_file_name
        
        # Ensure the directory exists
        excel_file_dir = excel_file_path.parent
        os.makedirs(excel_file_dir, exist_ok=True)
        
        with pd.ExcelWriter(str(excel_file_path), engine='xlsxwriter') as writer:
            for f_dir, data_types in f_dirs.items():
                for data_type, data in data_types.items():
                    avg_throughput = data['average']
                    max_throughput = data['max']
                    
                    # Prepare data for DataFrame
                    metrics = ['Average Throughput (MB/s)', 'Max Throughput (MB/s)']
                    values = [avg_throughput, max_throughput]
                    df = pd.DataFrame({
                        'Metric': metrics,
                        'Value': values
                    })
                    
                    # Define sheet name as "F1.0_Csv" or "F1.0_Parquet"
                    sheet_name = f"{f_dir}_{data_type}"
                    
                    # Ensure sheet name is within Excel's sheet name limit (31 characters)
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # Save to the respective sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  Added sheet '{sheet_name}' to {excel_file_path}")
        
        print(f"Summary data saved to {excel_file_path}\n")

def print_summary(sf_averages):
    """
    Print the arrays of throughput values and their averages.
    """
    print("\n===== Throughput Summary =====\n")
    for sf, f_dirs in sf_averages.items():
        for f_dir, data_types in f_dirs.items():
            for data_type, data in data_types.items():
                throughputs = data['throughputs']
                avg_throughput = data['average']
                max_throughput = data['max']
                print(f"SF: {sf}, F: {f_dir}, Data Type: {data_type}")
                print(f"Throughput Values: {throughputs}")
                print(f"Average Throughput: {avg_throughput:.2f} MB/s")
                print(f"Max Throughput: {max_throughput:.2f} MB/s\n")

def main():
    """
    Main function to process PCM stats files in the directory structure and save summaries to Excel files.
    """
    # Define the base PCM directory
    base_path = "Measures/Files"
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    
    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        return
    
    # Find all pcm_stats.txt files
    pcm_stats_files = find_pcm_stats_files(base_path)
    
    if not pcm_stats_files:
        print(f"No pcm_stats.txt files found under: {base_path}")
        return
    
    print(f"Found {len(pcm_stats_files)} pcm_stats.txt files to process.\n")
    
    # Aggregate throughput data
    sf_averages = aggregate_throughput(pcm_stats_files)
    
    # Print the summary of throughput values and averages
    print_summary(sf_averages)
    
    # Save the summary to Excel
    save_to_excel(base_path, sf_averages)

if __name__ == "__main__":
    main()
