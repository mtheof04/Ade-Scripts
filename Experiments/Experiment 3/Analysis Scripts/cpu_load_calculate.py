import pandas as pd
import re
import glob
import sys
import os

def main():
    # Define the expected columns in the CPU load log files
    CPU_COLUMNS = [
        'Time', 'CPU', '%usr', '%nice', '%sys', '%iowait',
        '%irq', '%soft', '%steal', '%guest', '%gnice', '%idle'
    ]

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    # Verify that the base_path exists
    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)

    # Dynamically find all SF folders within the base_path
    sf_folders = [
        sf for sf in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, sf)) and sf.startswith("SF")
    ]

    if not sf_folders:
        print("No SF folders found in the base path.")
        sys.exit(1)

    print(f"Found SF folders: {', '.join(sf_folders)}")

    # Function to calculate average CPU metrics from a log file and return them
    def calculate_average_metrics(file_path):
        try:
            # Read the CPU load log file
            df = pd.read_csv(
                file_path,
                sep=r'\s+',
                names=CPU_COLUMNS,
                skiprows=1,
                engine='python'
            )
        except Exception as e:
            print(f"Error reading '{file_path}': {e}")
            return None

        # Filter rows where the CPU column is 'all'
        filtered_df = df[df['CPU'] == 'all']

        # Exclude the first and last entries if there are four or more records
        if len(filtered_df) >= 4:
            filtered_df = filtered_df.iloc[1:-1]

        try:
            # Calculate the average of each desired CPU metric
            avg_usr = filtered_df['%usr'].astype(float).mean()
            avg_sys = filtered_df['%sys'].astype(float).mean()
            avg_iowait = filtered_df['%iowait'].astype(float).mean()
            avg_idle = filtered_df['%idle'].astype(float).mean()

            # Round the averages to two decimal places
            avg_metrics = {
                '%usr': round(avg_usr, 2),
                '%sys': round(avg_sys, 2),
                '%iowait': round(avg_iowait, 2),
                '%idle': round(avg_idle, 2)
            }

            return avg_metrics
        except Exception as e:
            print(f"Error processing data in '{file_path}': {e}")
            return None

    # Process each SF folder separately
    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)
        sf_name = os.path.basename(sf_folder_path)  # e.g., 'SF100'

        print(f"\nProcessing folder: {sf_name}")

        # Initialize lists to store frequency and thread information
        frequencies = []
        threads = []

        # Find all frequency folders (F folders) within the SF folder
        f_folders = glob.glob(os.path.join(sf_folder_path, 'F*'))
        if not f_folders:
            print(f"No frequency folders found in '{sf_folder_path}'. Skipping.")
            continue

        for f_folder in f_folders:
            freq = os.path.basename(f_folder).lower()  # e.g., 'F2.5' -> 'f2.5'
            frequencies.append(freq)

            # Find all thread folders (T folders) within the frequency folder
            t_folders = glob.glob(os.path.join(f_folder, 'T*'))
            if not t_folders:
                print(f"No thread folders found in '{f_folder}'. Skipping.")
                continue

            for t_folder in t_folders:
                thread = os.path.basename(t_folder).lower()  # e.g., 'T20' -> 't20'
                threads.append((freq, thread, t_folder))

        if not threads:
            print(f"No thread folders found in any frequency folders of '{sf_folder_path}'. Skipping.")
            continue

        # Define the path for the combined Excel file within the current SF folder
        combined_excel_path = os.path.join(
            sf_folder_path,
            f'cpu_utilization_combined.xlsx'
        )

        # Initialize the Excel writer
        with pd.ExcelWriter(combined_excel_path, engine='xlsxwriter') as writer:
            # Process each combination of frequency and thread
            for freq, thread, t_folder in threads:
                sheet_name = f'{freq.upper()}_{thread.upper()}'
                print(f"  Processing sheet: {sheet_name}")

                # Define the pattern to match the target CPU load files
                pattern = re.compile(
                    rf'{re.escape(t_folder)}/CpuLoad/Aggregated/cpu_load_{re.escape(freq)}_{sf_folder.lower()}_{re.escape(thread)}_q(\d+)\.txt'
                )

                # Glob pattern to find all relevant CPU load files
                run_files_pattern = os.path.join(
                    t_folder,
                    'CpuLoad',
                    'Aggregated',
                    f'cpu_load_{freq}_{sf_folder.lower()}_{thread}_q*.txt'
                )
                run_files = glob.glob(run_files_pattern)

                if not run_files:
                    print(f"    No CPU load files found for {sheet_name}. Skipping sheet.")
                    continue

                # Sort the run files based on the query number extracted from the filename
                try:
                    run_files_sorted = sorted(
                        run_files,
                        key=lambda x: int(re.search(r'q(\d+)', x).group(1))
                    )
                except Exception as e:
                    print(f"    Error sorting files for {sheet_name}: {e}")
                    continue

                # Initialize a list to store the data for the current sheet
                data = []

                for run_file in run_files_sorted:
                    match = pattern.search(run_file)
                    if match:
                        query_number = int(match.group(1))
                        avg_metrics = calculate_average_metrics(run_file)

                        if avg_metrics is not None:
                            data.append([
                                query_number,
                                avg_metrics['%usr'],
                                avg_metrics['%sys'],
                                avg_metrics['%iowait'],
                                avg_metrics['%idle']
                            ])
                        else:
                            print(f"      Failed to calculate averages for '{run_file}'.")
                    else:
                        print(f"      File '{run_file}' does not match the expected pattern. Skipping.")

                if not data:
                    print(f"    No valid data found for {sheet_name}. Skipping sheet.")
                    continue

                # Create a DataFrame from the collected data
                df = pd.DataFrame(
                    data,
                    columns=['Query', '%usr', '%sys', '%iowait', '%idle']
                )

                # Ensure queries are in numerical order
                df = df.sort_values(by='Query').reset_index(drop=True)

                try:
                    # Write the DataFrame to the Excel file with the specified sheet name
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"    Data for {sheet_name} added to '{combined_excel_path}'")
                except Exception as e:
                    print(f"    Error writing sheet '{sheet_name}' to Excel: {e}")

        print(f"All data for '{sf_folder}' has been saved to '{combined_excel_path}' with each frequency-thread pair in a separate sheet.")

if __name__ == "__main__":
    main()
