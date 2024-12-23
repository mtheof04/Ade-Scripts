import os
import pandas as pd
import re
import sys

def process_logs(base_path):
    # Locate all SF folders (e.g., SF100, SF200)
    sf_folders = [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d)) and d.startswith("SF")]

    if not sf_folders:
        print("No SF folders found.")
        return

    for sf_folder in sf_folders:
        # Extract the SF value (e.g., SF100 -> 100)
        sf_match = re.search(r'SF(\d+)', os.path.basename(sf_folder))
        sf_value = sf_match.group(1) if sf_match else "Unknown SF"

        # List all log files in this SF folder
        all_files = [os.path.join(root, file)
                     for root, _, files in os.walk(sf_folder)
                     for file in files if file == "filtered_sda_sdb_logs.txt"]

        if not all_files:
            print(f"No log files found in {sf_folder}.")
            continue

        summary_data = []

        for file_path in all_files:
            # Extract Frequency from the folder name (e.g., "F1.0" -> 1.0)
            frequency_match = re.search(r'F([\d.]+)', file_path)
            if frequency_match:
                frequency = float(frequency_match.group(1))  # Extract numeric frequency, e.g., 1.0
            else:
                print(f"Could not determine frequency for file: {file_path}")
                continue

            # Read and process the file
            try:
                df = pd.read_csv(file_path, delim_whitespace=True, skiprows=1)
                df = df[df["Device"].isin(["sda", "sdb"])]  # Filter for sda and sdb devices
                total_kb_read = df["kB_read"].sum()
                total_kb_wrtn = df["kB_wrtn"].sum()

                # Print the file being processed and its details
                print(f"Processing file: {file_path}")
                print(f"SF: {sf_value}, Frequency: {frequency}, kB_read: {total_kb_read}, kB_wrtn: {total_kb_wrtn}")

                # Append data to summary
                summary_data.append({
                    "Frequency": frequency,
                    "kB_read": total_kb_read,
                    "kB_wrtn": total_kb_wrtn
                })
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue

        if summary_data:
            # Create a DataFrame and group data by Frequency
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.groupby("Frequency", as_index=False).sum()
            summary_df = summary_df.sort_values("Frequency")

            # Save summary to an Excel file in the SF folder
            output_file = os.path.join(sf_folder, f"io_metrics_sf{sf_value}_summary.xlsx")
            summary_df.to_excel(output_file, index=False, header=["Frequency", "kB_read", "kB_wrtn"])
            print(f"Summary saved to {output_file}")
        else:
            print(f"No valid data found in {sf_folder}.")

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)

    process_logs(base_path)
