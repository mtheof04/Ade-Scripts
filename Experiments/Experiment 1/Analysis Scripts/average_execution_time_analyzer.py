import os
import re
import sys
import pandas as pd

def extract_execution_time(file_path):
    """Extract the Total Execution Time from the file."""
    print(f"Checking file: {file_path}")  # Debugging output
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if "Execution" in line:
                    print(f"Found line: {line.strip()}")
                
                match = re.search(r'Total Execution Time: (\d+)\s+seconds', line)
                
                if match:
                    return int(match.group(1))
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return 0
    return 0

def process_iteration_folder(base_path):
    """Process all iterations for a given base path and calculate average, sample std deviation, and std error of execution time."""
    total_time = 0
    execution_times = []
    iteration_pattern = re.compile(r'^Iteration(\d+)$')

    # Get all Iteration folders
    iterations = [folder for folder in os.listdir(base_path) if iteration_pattern.match(folder)]
    iterations.sort(key=lambda x: int(iteration_pattern.match(x).group(1)))  # Sort Iteration folders by number

    print(f"Found iteration folders: {iterations}")

    for iteration_folder in iterations:
        file_path = os.path.join(base_path, iteration_folder, "ilo_power_timestamps.txt")
        print(f"Looking for file: {file_path}")
        execution_time = extract_execution_time(file_path)
        iteration_number = iteration_pattern.match(iteration_folder).group(1)
        print(f"Iteration {iteration_number}: {execution_time} seconds")
        total_time += execution_time
        execution_times.append(execution_time)

    if len(execution_times) > 0:
        average_time = total_time / len(execution_times)
        std_dev_time = pd.Series(execution_times).std(ddof=1)  # Sample std deviation
        std_error_time = std_dev_time / (len(execution_times) ** 0.5)  # Standard error
    else:
        average_time = 0
        std_dev_time = 0
        std_error_time = 0

    print(f"Execution times for all iterations: {execution_times}")
    return average_time, std_dev_time, std_error_time, len(iterations)

def process_data_type_folders(f_folder_path, data_type_folder):
    """Process Csv or Parquet subfolder."""
    print(f"Processing data type folder: {data_type_folder}")
    average_time, std_dev_time, std_error_time, iteration_count = process_iteration_folder(data_type_folder)
    
    if iteration_count > 0:
        # Create DataFrame with the number of iterations, average time, sample std deviation, and std error
        df = pd.DataFrame([[f'{iteration_count} Iterations', average_time, std_dev_time, std_error_time]], 
                          columns=['Iterations', 'Average Time (s)', 'Sample Std Deviation (s)', 'Std Error (s)'])
        return df
    return None

def main(base_path):
    """Main function to process SF and F folders and combine results into one file for all combinations."""
    combined_data_all_folders = {}  # Dictionary to store data from all SF and F folders

    sf_pattern = re.compile(r'^SF\d+$')  # Pattern to match SF folders (e.g., SF100, SF300)

    # Find all SF folders in the base_path
    sf_folders = [folder for folder in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, folder)) and sf_pattern.match(folder)]
    sf_folders.sort()

    if not sf_folders:
        print(f"No SF folders found in base path: {base_path}")
        return

    print(f"Found SF folders: {sf_folders}")

    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)
        print(f"Processing SF folder: {sf_folder_path}")

        # Find all F folders within the current SF folder
        f_pattern = re.compile(r'^F\d+(\.\d+)?$')  # Matches F1, F1.0, F1.5, etc.
        f_folders = [folder for folder in os.listdir(sf_folder_path) if os.path.isdir(os.path.join(sf_folder_path, folder)) and f_pattern.match(folder)]
        f_folders.sort()

        if not f_folders:
            print(f"No F folders found in SF folder: {sf_folder_path}. Skipping.")
            continue

        print(f"Found F folders in {sf_folder}: {f_folders}")

        for f_folder in f_folders:
            f_folder_path = os.path.join(sf_folder_path, f_folder)
            print(f"Processing F folder: {f_folder_path}")

            # Process 'Csv' and 'Parquet' subfolders
            for data_type in ['Csv', 'Parquet']:
                data_type_folder = os.path.join(f_folder_path, data_type)

                if not os.path.exists(data_type_folder):
                    print(f"Warning: {data_type_folder} does not exist in {f_folder_path}. Skipping.")
                    continue

                print(f"Processing {data_type} folder in {f_folder_path}")
                df = process_data_type_folders(f_folder_path, data_type_folder)

                # Add data to combined_data if it's found
                if df is not None:
                    # Create a unique key combining SF folder, F folder, and data type
                    folder_key = f"{sf_folder}_{f_folder}_{data_type}"
                    combined_data_all_folders[folder_key] = df

        # Save the combined results for the current SF folder
        sf_combined_data = {key: value for key, value in combined_data_all_folders.items() if key.startswith(sf_folder)}

        if sf_combined_data:
            combined_output_path = os.path.join(sf_folder_path, f'query_execution_times_avg_{sf_folder}_combined.xlsx')
            with pd.ExcelWriter(combined_output_path, engine='xlsxwriter') as writer:
                for folder_key, data in sf_combined_data.items():
                    sheet_name = re.sub(r'^SF\d+_', '', folder_key)[:31]
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Combined results for {sf_folder} saved to {combined_output_path}")
        else:
            print(f"No data to save for {sf_folder}.")

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)

    main(base_path)
