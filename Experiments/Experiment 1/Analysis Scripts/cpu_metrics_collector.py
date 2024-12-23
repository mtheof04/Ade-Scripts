import os
import sys
import re
import pandas as pd

def extract_cpu_data(log_lines):
    cpu_data = []
    column_indices = {}

    for line in log_lines:
        line = line.strip()
        if not line:
            continue
        if 'Linux' in line or 'Average' in line:
            continue
        if 'CPU' in line and '%usr' in line:
            headers = line.split()
            for idx, col_name in enumerate(headers):
                column_indices[col_name.strip('%')] = idx
            continue
        columns = line.split()
        if 'all' in columns:
            idx_cpu = columns.index('all')
            time_value = columns[0] + ' ' + columns[1]
            try:
                usr_value = float(columns[column_indices.get('usr', idx_cpu + 1)])
                sys_value = float(columns[column_indices.get('sys', idx_cpu + 3)])
                iowait_value = float(columns[column_indices.get('iowait', idx_cpu + 4)])
                idle_value = float(columns[column_indices.get('idle', -1)])
            except (ValueError, KeyError, IndexError):
                continue
            cpu_data.append([time_value, usr_value, sys_value, iowait_value, idle_value])
    return pd.DataFrame(cpu_data, columns=["Time", "usr", "sys", "iowait", "idle"])

def process_and_collect(log_file, iteration_name):
    try:
        with open(log_file, 'r') as file:
            content = file.readlines()
    except Exception as e:
        print(f"Error reading file {log_file}: {e}")
        return None

    # Extract CPU data
    cpu_data_adjusted = extract_cpu_data(content)
    if cpu_data_adjusted.empty:
        print(f"No data found in {log_file}")
        return None

    return iteration_name, cpu_data_adjusted

# Main execution block
if __name__ == "__main__":
   
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    else:
        base_path = default_base_path

    print(f"Base Path: {base_path}")

    # Compile regex to match SF folders (e.g., SF100, SF300)
    sf_folder_pattern = re.compile(r'^SF\d+$')

    # Compile regex to match Iteration folders (e.g., Iteration1, Iteration2)
    iteration_folder_pattern = re.compile(r'^Iteration(\d+)$')

    # Process each SF folder
    try:
        sf_folders = [f for f in os.listdir(base_path)
                      if sf_folder_pattern.match(f) and os.path.isdir(os.path.join(base_path, f))]
    except FileNotFoundError:
        print(f"Error: Base path '{base_path}' does not exist.")
        sys.exit(1)

    if not sf_folders:
        print(f"No SF folders found in base path '{base_path}'.")
        sys.exit(1)

    print(f"Found SF folders: {sf_folders}")

    # Process each SF folder
    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)
        print(f"\nProcessing SF folder: {sf_folder_path}")

        # Prepare Excel writer for the SF folder
        output_excel_file = os.path.join(sf_folder_path, f"cpu_metrics_{sf_folder}_combined.xlsx")
        with pd.ExcelWriter(output_excel_file) as writer:

            # Detect F folders
            try:
                f_folders = [f for f in os.listdir(sf_folder_path)
                             if re.match(r'^F\d+(\.\d+)?$', f) and os.path.isdir(os.path.join(sf_folder_path, f))]
            except FileNotFoundError:
                print(f"Warning: Could not list F folders in {sf_folder_path}. Skipping.")
                continue

            if not f_folders:
                print(f"No F folders found in {sf_folder_path}. Skipping.")
                continue

            print(f"F folders to process: {f_folders}")

            # Process each F folder
            for f_folder in f_folders:
                f_folder_path = os.path.join(sf_folder_path, f_folder)
                print(f"\nProcessing F folder: {f_folder_path}")

                # Loop through 'Csv' and 'Parquet' subfolders
                for data_type in ['Csv', 'Parquet']:
                    data_type_folder = os.path.join(f_folder_path, data_type)
                    if not os.path.exists(data_type_folder):
                        print(f"Warning: {data_type_folder} does not exist. Skipping.")
                        continue

                    print(f"Processing data type folder: {data_type_folder}")

                    # Find all Iteration folders inside the data type folder
                    try:
                        iteration_folders = [
                            os.path.join(data_type_folder, subfolder)
                            for subfolder in os.listdir(data_type_folder)
                            if iteration_folder_pattern.match(subfolder) and os.path.isdir(os.path.join(data_type_folder, subfolder))
                        ]
                    except FileNotFoundError:
                        print(f"Warning: Could not list directories in {data_type_folder}. Skipping.")
                        continue

                    if not iteration_folders:
                        print(f"No Iteration folders found in {data_type_folder}. Skipping.")
                        continue

                    # Sort iteration folders numerically by the number in Iteration
                    iteration_folders = sorted(
                        iteration_folders,
                        key=lambda x: int(iteration_folder_pattern.match(os.path.basename(x)).group(1))
                    )

                    print(f"Found iteration folders: {iteration_folders}")

                    # Cumulative table for combined stats
                    combined_table = pd.DataFrame()

                    # Process iterations
                    for iteration_folder in iteration_folders:
                        iteration_name = os.path.basename(iteration_folder)
                        log_file = os.path.join(iteration_folder, "mpstat.log")

                        if not os.path.exists(log_file):
                            print(f"Log file not found in {iteration_folder}. Skipping.")
                            continue

                        # Collect data for this iteration
                        result = process_and_collect(log_file, iteration_name)
                        if result is None:
                            continue

                        _, stats_df = result
                        combined_table = pd.concat([combined_table, stats_df], ignore_index=True)

                    # Print combined table
                    if not combined_table.empty:
                        print(f"\nCombined Table for {data_type_folder}:\n")
                        print(combined_table)

                        # Calculate Min, Max, and Averages for columns
                        min_stats = combined_table[["usr", "sys", "iowait", "idle"]].min().to_frame().T
                        min_stats.insert(0, "Metric", "Min")

                        max_stats = combined_table[["usr", "sys", "iowait", "idle"]].max().to_frame().T
                        max_stats.insert(0, "Metric", "Max")

                        avg_stats = combined_table[["usr", "sys", "iowait", "idle"]].mean().to_frame().T
                        avg_stats.insert(0, "Metric", "Average")

                        # Combine all stats into a single DataFrame
                        combined_stats = pd.concat([min_stats, max_stats, avg_stats], ignore_index=True)

                        # Print the formatted stats table
                        print(f"\nFormatted Metrics for {data_type_folder}:\n")
                        print(combined_stats)

                        # Save the formatted metrics to the respective Excel sheet
                        sheet_name = f"{f_folder} {data_type}"
                        combined_stats.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"Metrics (Min, Max, Average) saved to sheet: {sheet_name}")

        print(f"Excel file saved: {output_excel_file}")
