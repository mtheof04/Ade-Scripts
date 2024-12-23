import os
import sys
import pandas as pd
import re

if len(sys.argv) > 1:
    base_path = sys.argv[1]

# Function to read and collect CPU data from log lines
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
        if len(columns) < len(column_indices):
            continue
        if 'all' in columns:
            idx_cpu = columns.index('all')
            time_value = columns[0] + ' ' + columns[1]
            try:
                usr_value = float(columns[column_indices.get('usr', idx_cpu + 1)].replace(',', '.'))
                sys_value = float(columns[column_indices.get('sys', idx_cpu + 2)].replace(',', '.'))
                iowait_value = float(columns[column_indices.get('iowait', idx_cpu + 3)].replace(',', '.'))
                idle_value = float(columns[column_indices.get('idle', idx_cpu + 4)].replace(',', '.'))
            except (ValueError, KeyError, IndexError):
                continue
            cpu_data.append([time_value, usr_value, sys_value, iowait_value, idle_value])
    return pd.DataFrame(cpu_data, columns=["Time", "usr", "sys", "iowait", "idle"])

# Function to extract frequency from F folder name
def extract_frequency(f_folder_name):
    match = re.search(r'F(\d+(\.\d+)?)', f_folder_name, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            print(f"Warning: Unable to convert frequency from folder name '{f_folder_name}'. Defaulting to 1.0.")
            return 1.0
    else:
        print(f"Warning: Frequency not found in folder name '{f_folder_name}'. Defaulting to 1.0.")
        return 1.0

# Function to handle log processing and collect data
def process_and_collect(log_file, sub_folder):
    print(f"Processing log file: {log_file} in folder {sub_folder}")
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

    # Add the phase (e.g., Sorting, Filtering) to the DataFrame
    cpu_data_adjusted['Phase'] = sub_folder.capitalize()
    return cpu_data_adjusted

def process_iteration_folder(iteration_folder):
    print(f"Processing iteration folder: {iteration_folder}")
    all_cpu_stats = {}

    # Look for mpstat.log directly inside the Iteration folder
    direct_log_file = os.path.join(iteration_folder, "mpstat.log")
    if os.path.exists(direct_log_file):
        iteration_name = os.path.basename(iteration_folder)
        stats_df = process_and_collect(direct_log_file, iteration_name)
        if stats_df is not None and not stats_df.empty:
            all_cpu_stats[iteration_name] = [stats_df]
    else:
        # Check for subfolders (e.g., Aggregations, Filtering)
        sub_folders = [
            subfolder for subfolder in os.listdir(iteration_folder)
            if os.path.isdir(os.path.join(iteration_folder, subfolder))
        ]

        # Process each subfolder
        for sub_folder in sub_folders:
            full_folder_path = os.path.join(iteration_folder, sub_folder)
            log_file = os.path.join(full_folder_path, "mpstat.log")
            if os.path.exists(log_file):
                result = process_and_collect(log_file, sub_folder)
                if result is not None and not result.empty:
                    label, stats_df = sub_folder, result
                    if label not in all_cpu_stats:
                        all_cpu_stats[label] = [stats_df]
                    else:
                        all_cpu_stats[label].append(stats_df)
            else:
                print(f"Log file not found: {log_file}")

    return all_cpu_stats

# Function to calculate min, max, and averages for a group of CPU stats
def calculate_aggregates(cpu_stats):
    combined_stats = {}

    for label, dfs in cpu_stats.items():
        combined_df = pd.concat(dfs, ignore_index=True)

        grouped = combined_df.groupby('Phase', as_index=False)

        aggregated_list = []
        for phase, phase_data in grouped:
            combined_avg = phase_data.mean(numeric_only=True).to_frame().T
            combined_avg.insert(0, 'Metric', 'Average')
            combined_avg.insert(0, 'Phase', phase)

            combined_min = phase_data.min(numeric_only=True).to_frame().T
            combined_min.insert(0, 'Metric', 'Min')
            combined_min.insert(0, 'Phase', phase)

            combined_max = phase_data.max(numeric_only=True).to_frame().T
            combined_max.insert(0, 'Metric', 'Max')
            combined_max.insert(0, 'Phase', phase)

            aggregated_list.append(pd.concat([combined_min, combined_max, combined_avg], ignore_index=True))

        combined_stats[label] = pd.concat(aggregated_list, ignore_index=True)

    return combined_stats

# Function to find all SF folders dynamically
def find_sf_folders(base_path):
    return [
        folder for folder in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, folder)) and folder.startswith("SF")
    ]

# Function to process all F folders within a given SF folder
def process_sf_folder(sf_folder_path, sf_folder_name, aggregated_data):
    print(f"\nProcessing SF folder: {sf_folder_name}")
    f_folders = [
        folder for folder in os.listdir(sf_folder_path)
        if os.path.isdir(os.path.join(sf_folder_path, folder)) and folder.startswith("F")
    ]

    if not f_folders:
        print(f"No F folders found in {sf_folder_name}. Skipping.")
        return

    for f_folder in f_folders:
        f_folder_path = os.path.join(sf_folder_path, f_folder)
        iterations_path = os.path.join(f_folder_path, 'Iterations')

        if not os.path.exists(iterations_path):
            print(f"Warning: Folder {iterations_path} does not exist. Skipping.")
            continue

        print(f"Processing F folder: {f_folder_path}")

        frequency = extract_frequency(f_folder)
        print(f"Extracted Frequency: {frequency}")

        iteration_folders = [
            os.path.join(iterations_path, subfolder)
            for subfolder in os.listdir(iterations_path)
            if os.path.isdir(os.path.join(iterations_path, subfolder)) and subfolder.startswith("Iteration")
        ]

        iteration_folders = sorted(
            iteration_folders,
            key=lambda x: int(''.join(filter(str.isdigit, os.path.basename(x))) or 0)
        )

        all_cpu_data = {}
        for iteration_folder in iteration_folders:
            iteration_name = os.path.basename(iteration_folder)
            print(f"Processing {iteration_name}...")

            iteration_cpu_stats = process_iteration_folder(iteration_folder)

            if not iteration_cpu_stats:
                print(f"No CPU stats found for {iteration_name}")
                continue

            for label, dfs in iteration_cpu_stats.items():
                if label not in all_cpu_data:
                    all_cpu_data[label] = dfs
                else:
                    all_cpu_data[label].extend(dfs)

        if all_cpu_data:
            combined_stats = calculate_aggregates(all_cpu_data)

            for label, df in combined_stats.items():
                df.insert(0, 'Frequency', frequency)
                df['Frequency'] = df['Frequency'].replace({frequency: f"{frequency}"})

                df = df[['Frequency', 'Phase', 'Metric', 'usr', 'sys', 'iowait', 'idle']]

                aggregated_data.append(df)

# Main processing starts here
def main():
    aggregated_data = []

    sf_folders = find_sf_folders(base_path)

    if not sf_folders:
        print(f"No SF folders found in base path: {base_path}")
        sys.exit(1)

    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)
        process_sf_folder(sf_folder_path, sf_folder, aggregated_data)

        if not aggregated_data:
            print(f"No CPU metrics were processed for {sf_folder}. Skipping Excel file creation.")
            continue

        # Combine all aggregated DataFrames into a single DataFrame
        final_df = pd.concat(aggregated_data, ignore_index=True)

        # Sort and format the DataFrame
        final_df['Frequency'] = final_df['Frequency'].astype(float)
        final_df = final_df.sort_values(by=['Frequency', 'Phase', 'Metric'], ascending=[True, True, True])
        final_df['Frequency'] = final_df['Frequency'].apply(lambda x: int(x) if x.is_integer() else x)

        # Define the Excel file path inside the current SF folder
        excel_file_path = os.path.join(sf_folder_path, "cpu_metrics_combined.xlsx")

        # Write the final DataFrame to one Excel sheet
        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, sheet_name='CPU_Metrics', index=False)

        print(f"\nAll combined CPU Metrics have been saved to {excel_file_path}")
        print(f"Processing for {sf_folder} completed.")

    print("\nAll SF folders have been processed.")


if __name__ == "__main__":
    main()
