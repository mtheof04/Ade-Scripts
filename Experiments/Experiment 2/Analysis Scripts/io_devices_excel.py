import os
import sys
import pandas as pd

def parse_iterations(file_path):
    """Parse the filtered logs file and extract data."""
    with open(file_path, 'r') as file:
        lines = file.readlines()

    phase_data = {}
    current_phase = None
    
    for line in lines:
        line = line.strip()
        # Detect phase names
        if line in {"Aggregations", "Filtering", "Joins", "Sequential", "Sorting"}:
            current_phase = line
            if current_phase not in phase_data:
                phase_data[current_phase] = {"kB_read": 0, "kB_wrtn": 0}
        # Process data lines
        elif line.startswith("sdb") and current_phase:
            columns = line.split()
            try:
                kB_read = float(columns[5])
                kB_wrtn = float(columns[6])
                phase_data[current_phase]["kB_read"] += kB_read
                phase_data[current_phase]["kB_wrtn"] += kB_wrtn
            except (IndexError, ValueError):
                continue  # Ignore lines that don't match expected format
    
    return phase_data

def process_logs(base_path):
    """Process all logs and create an Excel file."""
    sf_folders = [folder for folder in os.listdir(base_path) if folder.startswith("SF") and os.path.isdir(os.path.join(base_path, folder))]
    for sf_folder in sf_folders:
        sf_path = os.path.join(base_path, sf_folder)
        f_folders = [folder for folder in os.listdir(sf_path) if folder.startswith("F") and os.path.isdir(os.path.join(sf_path, folder))]
        excel_data = {}

        for f_folder in f_folders:
            f_path = os.path.join(sf_path, f_folder)
            file_path = os.path.join(f_path, "filtered_sda_sdb_include_first_iteration_logs.txt")
            if os.path.exists(file_path):
                phase_data = parse_iterations(file_path)
                # Convert phase data to a DataFrame
                df = pd.DataFrame([
                    {"Phase": phase, "kB_read": metrics["kB_read"], "kB_wrtn": metrics["kB_wrtn"]}
                    for phase, metrics in phase_data.items()
                ])
                excel_data[f_folder] = df

        # Save to Excel file
        if excel_data:
            output_file = os.path.join(sf_path, f"io_metrics.xlsx")
            with pd.ExcelWriter(output_file) as writer:
                for sheet_name, data in excel_data.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Excel file created: {output_file}")

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]
        
    if not os.path.exists(base_path):
        print(f"Error: Base path '{base_path}' does not exist.")
    else:
        process_logs(base_path)
