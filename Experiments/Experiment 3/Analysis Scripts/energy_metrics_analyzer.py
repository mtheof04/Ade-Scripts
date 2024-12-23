import pandas as pd
import os
import sys
import re

def find_sf_folders(base_path):
    """
    Discover all SF folders (e.g., SF100, SF300) within the base_path.
    
    Args:
        base_path (str): The base directory path.
    
    Returns:
        list: A list of SF folder names.
    """
    sf_folders = [f for f in os.listdir(base_path) 
                 if re.match(r'^SF\d+$', f) and os.path.isdir(os.path.join(base_path, f))]
    print(f"Discovered SF folders: {sf_folders}")
    return sf_folders

def process_sf_folder(sf_folder_path, sf_number):
    """
    Process the power and query metrics for a single SF folder and generate the energy metrics.
    
    Args:
        sf_folder_path (str): Path to the SF folder.
        sf_number (str): The SF number extracted from the folder name.
    """
    # Define paths to the files in the SF folder
    power_metrics_file_path = os.path.join(sf_folder_path, f'power_metrics_combined.xlsx')
    query_times_file_path = os.path.join(sf_folder_path, f'query_execution_times_avg_combined.xlsx')
    
    # Check if the required files exist
    if not os.path.exists(power_metrics_file_path):
        print(f"Warning: Power metrics file not found at {power_metrics_file_path}. Skipping SF{sf_number}.")
        return
    if not os.path.exists(query_times_file_path):
        print(f"Warning: Query times file not found at {query_times_file_path}. Skipping SF{sf_number}.")
        return
    
    print(f"\nProcessing SF{sf_number}:")
    print(f"Power Metrics File: {power_metrics_file_path}")
    print(f"Query Times File: {query_times_file_path}")
    
    # Load all sheets from both Excel files
    power_metrics_sheets = pd.read_excel(power_metrics_file_path, sheet_name=None)
    query_times_sheets = pd.read_excel(query_times_file_path, sheet_name=None)
    
    # Prepare to write to a new Excel file with multiple sheets
    output_file = os.path.join(sf_folder_path, f'energy_metrics_combined.xlsx')
    with pd.ExcelWriter(output_file) as writer:
        for sheet_name, power_metrics_df in power_metrics_sheets.items():
            # Check and rename columns for clarity
            if "Overall Average Power (Watts)" in power_metrics_df.columns:
                power_metrics_df = power_metrics_df.rename(columns={
                    "Overall Average Power (Watts)": "Workload Power"
                })
            if "Query" not in power_metrics_df.columns:
                print(f"Warning: 'Query' column missing in sheet '{sheet_name}' of SF{sf_number}. Skipping this sheet.")
                continue
    
            # Get the corresponding sheet data from the query times file
            query_times_df = query_times_sheets.get(sheet_name)
            if query_times_df is None:
                print(f"Warning: Matching sheet '{sheet_name}' not found in query times file for SF{sf_number}. Skipping this sheet.")
                continue
    
            # Rename columns in query_times_df
            if "Average Time(s)" in query_times_df.columns:
                query_times_df = query_times_df.rename(columns={"Average Time(s)": "Avg Time (s)"})
            
            # Check if 'Query' column exists in query_times_df
            if "Query" not in query_times_df.columns or "Avg Time (s)" not in query_times_df.columns:
                print(f"Warning: Required columns missing in query times sheet '{sheet_name}' for SF{sf_number}. Skipping this sheet.")
                continue
    
            # Merging the two DataFrames on 'Query' column
            combined_df = pd.merge(
                power_metrics_df[["Query", "Workload Power"]],
                query_times_df[["Query", "Avg Time (s)"]],
                on="Query",
                how="inner"  # Ensures only matching queries are merged
            )
    
            # Calculate the 'Energy' column as Workload Power * Avg Time (s)
            combined_df["Energy (Joules)"] = combined_df["Workload Power"] * combined_df["Avg Time (s)"]
    
            # Write each combined DataFrame to a new sheet in the output file
            combined_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Excel sheet name limit is 31 characters
    
    print(f"Combined energy metrics file created at {output_file}")

def main():
    """
    Main function to process multiple SF folders and generate combined energy metrics.
    """
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    
    # Check if the base_path exists
    if not os.path.exists(base_path):
        print(f"Error: The base_path '{base_path}' does not exist.")
        sys.exit(1)
    
    # Discover all SF folders within the base_path
    sf_folders = find_sf_folders(base_path)
    
    if not sf_folders:
        print(f"No SF folders found in the base_path '{base_path}'. Exiting.")
        sys.exit(1)
    
    # Process each SF folder
    for sf_folder in sf_folders:
        sf_number_match = re.match(r'^SF(\d+)$', sf_folder)
        if sf_number_match:
            sf_number = sf_number_match.group(1)
            sf_folder_path = os.path.join(base_path, sf_folder)
            process_sf_folder(sf_folder_path, sf_number)
        else:
            print(f"Skipping unrecognized folder: {sf_folder}")  # Debug

if __name__ == "__main__":
    main()
