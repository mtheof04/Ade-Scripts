import os
import sys
import pandas as pd
from natsort import natsorted  # For natural sorting

def parse_iterations(file_path):
    """Parse the filtered logs file and extract data by query."""
    with open(file_path, 'r') as file:
        lines = file.readlines()

    query_data = {}
    current_query = None
    
    for line in lines:
        line = line.strip()
        # Detect query start
        if line.startswith("Running TPC-H Query"):
            query_number = line.split()[3].replace("Query", "").strip()
            current_query = f"Query {query_number}"
            if current_query not in query_data:
                query_data[current_query] = {"kB_read": 0, "kB_wrtn": 0}
        # Process data lines for sdb
        elif current_query and line.startswith("sdb"):
            columns = line.split()
            try:
                kB_read = float(columns[5])
                kB_wrtn = float(columns[6])
                query_data[current_query]["kB_read"] += kB_read
                query_data[current_query]["kB_wrtn"] += kB_wrtn
            except (IndexError, ValueError):
                continue  # Ignore lines that don't match expected format

    return query_data

def process_logs(base_path):
    """Process all logs and create an Excel file."""
    sf_folders = [folder for folder in os.listdir(base_path) if folder.startswith("SF") and os.path.isdir(os.path.join(base_path, folder))]
    for sf_folder in sf_folders:
        sf_path = os.path.join(base_path, sf_folder)
        f_folders = [folder for folder in os.listdir(sf_path) if folder.startswith("F") and os.path.isdir(os.path.join(sf_path, folder))]
        excel_data = {}

        for f_folder in f_folders:
            t48_path = os.path.join(sf_path, f_folder, "T48")
            # Exclude files starting with 'filtered_sda_sdb_skip_first_iteration'
            filtered_files = [
                file for file in os.listdir(t48_path)
                if file.startswith("filtered_sda_sdb") and not file.startswith("filtered_sda_sdb_skip_first_iteration")
            ]
            
            for filtered_file in filtered_files:
                file_path = os.path.join(t48_path, filtered_file)
                if os.path.exists(file_path):
                    query_data = parse_iterations(file_path)
                    # Convert query data to a DataFrame and sort naturally
                    sorted_data = natsorted(query_data.items())
                    df = pd.DataFrame([
                        {"Query": query, "kB_read": metrics["kB_read"], "kB_wrtn": metrics["kB_wrtn"]}
                        for query, metrics in sorted_data
                    ])
                    # Use the F folder name as the sheet name
                    sheet_name = f_folder[:31]  # Truncate if needed
                    excel_data[sheet_name] = df

        # Save to Excel file
        if excel_data:
            output_file = os.path.join(sf_path, f"io_metrics.xlsx")
            with pd.ExcelWriter(output_file) as writer:
                for sheet_name, data in excel_data.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Excel file created: {output_file}")

if __name__ == "__main__":

    # Determine the base_path from command-line arguments
    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    process_logs(base_path)
