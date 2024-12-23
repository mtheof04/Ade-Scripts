import os
import glob
import re
import sys
import pandas as pd

def process_subdirectories(perf_stat_dir):
    """
    Process subdirectories (e.g., Filtering, Aggregations) in the PCM directory.
    Extract relevant data from pcm_stats.txt files.
    """
    subdirs = [d for d in os.listdir(perf_stat_dir) if os.path.isdir(os.path.join(perf_stat_dir, d))]
    throughput_summary = {}

    for subdir in subdirs:
        subdir_path = os.path.join(perf_stat_dir, subdir)
        pcm_file = os.path.join(subdir_path, "pcm_stats.txt")
        
        if not os.path.exists(pcm_file):
            print(f"Warning: File not found - {pcm_file}")
            continue
        
        print(f"Reading file: {pcm_file}")
        with open(pcm_file, 'r') as file:
            lines = file.readlines()
        
        # Extract System Memory Throughput(MB/s)
        throughput = extract_throughput(lines)
        if throughput:
            avg_throughput = sum(throughput) / len(throughput)
            print(f"System Memory Throughput(MB/s) from {pcm_file}: {throughput}")
            print(f"\nAverage System Memory Throughput(MB/s) for {pcm_file}: {avg_throughput:.2f}\n")
            throughput_summary[subdir] = avg_throughput

    return throughput_summary

def extract_throughput(lines):
    """
    Extract 'System Memory Throughput(MB/s):' values from pcm_stats.txt.
    """
    throughput = []
    for line in lines:
        match = re.search(r"System Memory Throughput\(MB/s\):\s+([\d.]+)", line)
        if match:
            throughput.append(float(match.group(1)))
    return throughput

def save_to_excel(output_path, summary_data):
    """
    Save the throughput summary data to an Excel file with sheets for each F* directory.
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for directory, data in summary_data.items():
            df = pd.DataFrame(list(data.items()), columns=['Phase', 'Average Throughput (MB/s)'])
            df.to_excel(writer, sheet_name=directory, index=False)
    print(f"Summary data saved to {output_path}")

def main(base_path):
    """
    Main function to traverse directories and extract throughput values.
    """
    search_pattern = os.path.join(base_path, 'SF*', 'F*', 'PCM')
    perf_stat_dirs = glob.glob(search_pattern)
    
    if not perf_stat_dirs:
        print(f"No PCM directories found matching pattern: {search_pattern}")
        return
    
    sf_summaries = {}
    for perf_stat_dir in perf_stat_dirs:
        print(f"Processing PCM directory: {perf_stat_dir}")
        throughput_summary = process_subdirectories(perf_stat_dir)
        sf_directory = os.path.dirname(os.path.dirname(perf_stat_dir))  # Extract the SF* directory path
        f_directory = os.path.basename(os.path.dirname(perf_stat_dir))  # Extract the F* directory name
        
        if sf_directory not in sf_summaries:
            sf_summaries[sf_directory] = {}
        sf_summaries[sf_directory][f_directory] = throughput_summary
    
    # Save an Excel file in each SF* folder
    for sf_dir, summary_data in sf_summaries.items():
        output_file = os.path.join(sf_dir, "memory_bandwidth.xlsx")
        save_to_excel(output_file, summary_data)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    main(base_path)
