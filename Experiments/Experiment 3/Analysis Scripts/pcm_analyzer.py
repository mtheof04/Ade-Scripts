import os
import sys
import glob
import re
import pandas as pd

def find_pcm_directories(base_path):
    """
    Find all PCM directories matching the pattern \SF*\F*\T*\PCM under the base path.
    """
    search_pattern = os.path.join(base_path, 'SF*', 'F*', 'T*', 'PCM')
    pcm_directories = glob.glob(search_pattern)
    return pcm_directories

def process_pcm_files(perf_stat_dir):
    """
    Process PCM directory and extract throughput from pcm_f*.txt files.
    Returns a dictionary mapping query numbers to average throughput.
    """
    # Regex pattern to match files like pcm_f1.0_sf100_t48_q1.txt to pcm_f1.0_sf100_t48_q22.txt
    pattern = re.compile(r"pcm_f[\d\.]+_sf\d+_t\d+_q(\d+)\.txt", re.IGNORECASE)
    
    throughput = {}
    
    # List all files in the PCM directory
    for file_name in os.listdir(perf_stat_dir):
        match = pattern.match(file_name)
        if match:
            query_num = int(match.group(1))
            file_path = os.path.join(perf_stat_dir, file_name)
            
            if not os.path.exists(file_path):
                print(f"Warning: File not found - {file_path}")
                throughput[query_num] = None
                continue
            
            print(f"Reading file: {file_path}")
            with open(file_path, 'r') as f:
                lines = f.readlines()
            
            # Extract System Memory Throughput(MB/s)
            throughput_values = extract_throughput(lines)
            if throughput_values:
                avg_throughput = sum(throughput_values) / len(throughput_values)
                print(f"System Memory Throughput(MB/s) from {file_path}: {throughput_values}")
                print(f"Average System Memory Throughput(MB/s) for Query {query_num}: {avg_throughput:.2f}\n")
                throughput[query_num] = avg_throughput
            else:
                print(f"No throughput data found in {file_path}")
                throughput[query_num] = None
    
    # Ensure all queries from 1 to 22 are present, even if some are missing
    for q in range(1, 23):
        if q not in throughput:
            throughput[q] = None  # or set to 0 or another default value
    
    return throughput

def extract_throughput(lines):
    """
    Extract 'System Memory Throughput(MB/s):' values from PCM file.
    """
    throughput = []
    for line in lines:
        match = re.search(r"System Memory Throughput\(MB/s\):\s+([\d.]+)", line)
        if match:
            throughput.append(float(match.group(1)))
    return throughput

def save_to_excel(output_path, sheets_data):
    """
    Save the throughput data to an Excel file with separate sheets for each F* directory.
    Each sheet contains 'Query' and 'Average Throughput (MB/s)' columns.
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, throughput_data in sheets_data.items():
            # Create a sorted list of queries from 1 to 22
            queries = list(range(1, 23))
            
            # Prepare data for DataFrame
            data = {
                'Query': queries,
                'Average Throughput (MB/s)': [throughput_data.get(q, None) for q in queries]
            }
            
            df = pd.DataFrame(data)
            
            # Save to the respective sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Added sheet '{sheet_name}' to {output_path}")
    
    print(f"Summary data saved to {output_path}\n")

def main():
    """
    Main function to process PCM files in all matching directories and save summaries to Excel files.
    """

    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    
    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        return
    
    # Find all PCM directories matching \SF*\F*\T*\PCM
    pcm_directories = find_pcm_directories(base_path)
    
    if not pcm_directories:
        print(f"No PCM directories found matching pattern under: {base_path}")
        return
    
    print(f"Found {len(pcm_directories)} PCM directories to process.\n")
    
    # Organize PCM directories by SF* and F* directories
    sf_f_map = {}
    
    for pcm_dir in pcm_directories:
        # Extract SF* and F* directory names
        parts = os.path.normpath(pcm_dir).split(os.sep)
        try:
            sf_dir = parts[-4]  # SF*
            f_dir = parts[-3]    # F*
        except IndexError:
            print(f"Warning: Unexpected directory structure for '{pcm_dir}'")
            continue
        
        if sf_dir not in sf_f_map:
            sf_f_map[sf_dir] = {}
        if f_dir not in sf_f_map[sf_dir]:
            sf_f_map[sf_dir][f_dir] = []
        sf_f_map[sf_dir][f_dir].append(pcm_dir)
    
    # Process each SF* directory
    for sf_dir, f_dirs in sf_f_map.items():
        sheets_data = {}
        
        for f_dir, pcm_dirs in f_dirs.items():
            # Aggregate throughput data for each F* across all T* directories
            aggregated_throughput = {}
            
            for pcm_dir in pcm_dirs:
                throughput = process_pcm_files(pcm_dir)
                
                # Aggregate by collecting all throughput values per query
                for query, value in throughput.items():
                    if query not in aggregated_throughput:
                        aggregated_throughput[query] = []
                    if value is not None:
                        aggregated_throughput[query].append(value)
            
            # Compute average throughput per query across all T* directories
            avg_throughput = {}
            for query in range(1, 23):
                values = aggregated_throughput.get(query, [])
                if values:
                    avg_value = sum(values) / len(values)
                    avg_throughput[query] = avg_value
                else:
                    avg_throughput[query] = None  # or set to 0
            
            sheets_data[f_dir] = avg_throughput
        
        excel_file_name = "memory_bandwidth.xlsx"
        output_file = os.path.join(base_path, sf_dir, excel_file_name)
        
        # Save the summary to Excel
        save_to_excel(output_file, sheets_data)

if __name__ == "__main__":
    main()
