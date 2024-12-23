import pandas as pd
import numpy as np
import re
import glob
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

if len(sys.argv) > 1:
    base_path = sys.argv[1]

# Verify that the base_path exists
if not os.path.exists(base_path):
    logging.error(f"Base path '{base_path}' does not exist. Please provide a valid path.")
    sys.exit(1)

# Find all SF folders dynamically (e.g., SF100, SF300, etc.)
sf_folders = glob.glob(os.path.join(base_path, 'SF*'))

if not sf_folders:
    logging.warning(f"No SF folders found in the base path '{base_path}'. Exiting.")
    sys.exit(0)

for sf_folder in sf_folders:
    sf = os.path.basename(sf_folder).lower()  # e.g., 'SF100' -> 'sf100'

    # Initialize lists to store frequency, thread, and extracted times data for the current SF
    summary_data = []

    # Find all F folders within the current SF folder
    f_folders = glob.glob(os.path.join(sf_folder, 'F*'))

    if not f_folders:
        logging.warning(f"No F folders found in '{sf_folder}'. Skipping this SF folder.")
        continue

    for f_folder in f_folders:
        freq = os.path.basename(f_folder).lower()  # e.g., 'F2.5' -> 'f2.5'

        # Find all T folders within the current F folder
        t_folders = glob.glob(os.path.join(f_folder, 'T*'))

        if not t_folders:
            logging.warning(f"No T folders found in '{f_folder}'. Skipping this F folder.")
            continue

        for t_folder in t_folders:
            thread = os.path.basename(t_folder).lower()  # e.g., 'T20' -> 't20'

            # Path to QueriesTiming folder inside each frequency-thread combination
            queries_timing_path = os.path.join(t_folder, 'QueriesTiming')

            if not os.path.exists(queries_timing_path):
                logging.warning(f"'QueriesTiming' folder does not exist in '{t_folder}'. Skipping.")
                continue

            # Process each file in the QueriesTiming folder
            query_files = glob.glob(os.path.join(queries_timing_path, 'query_*.txt'))

            if not query_files:
                logging.warning(f"No query files found in '{queries_timing_path}'. Skipping.")
                continue

            for query_file in query_files:
                # Extract only the query number, e.g., from "query_f2.0_sf100_t20_q1.txt" to "1"
                query_match = re.search(r'q(\d+)', os.path.basename(query_file))
                if query_match:
                    query_number = int(query_match.group(1))
                else:
                    logging.warning(f"Filename '{query_file}' does not match the expected pattern. Skipping.")
                    continue

                # Extract run times from each file
                run_times = []
                try:
                    with open(query_file, 'r') as file:
                        for line in file:
                            match = re.search(r"Run \d+ time: ([\d.]+)s", line)
                            if match:
                                run_times.append(float(match.group(1)))
                except Exception as e:
                    logging.error(f"Error reading '{query_file}': {e}")
                    continue

                # Print the run times for the current query
                logging.info(f"SF: {sf.upper()}, Frequency: {freq[1:]}, Thread: {thread[1:]}, Query {query_number}: {run_times}")

                # Calculate statistics: average, sample standard deviation, and standard error
                if run_times:
                    avg_time = np.mean(run_times)
                    sample_std_dev = np.std(run_times, ddof=1)  # Sample standard deviation
                    std_error = sample_std_dev / np.sqrt(len(run_times))  # Standard error
                else:
                    avg_time = sample_std_dev = std_error = None

                # Append the summary data for this query file
                summary_data.append({
                    'Frequency': float(freq[1:]),                # Convert 'f2.5' -> 2.5
                    'Thread': int(thread[1:]),                   # Convert 't20' -> 20
                    'Query': query_number,                       # Use only the query number, e.g., 1
                    'Average Time(s)': round(avg_time, 2) if avg_time is not None else None,
                    'Sample Std Dev(s)': round(sample_std_dev, 2) if sample_std_dev is not None else None,
                    'Std Error(s)': round(std_error, 2) if std_error is not None else None
                })

    # If no data was collected for this SF folder, skip it
    if not summary_data:
        logging.info(f"No data found for '{sf_folder}'. Skipping.")
        continue

    # Create a DataFrame to store the summary results for the current SF
    summary_df = pd.DataFrame(summary_data)

    # Sort by Frequency, Thread, and Query
    summary_df.sort_values(by=['Frequency', 'Thread', 'Query'], inplace=True)

    # Output file for the current SF folder
    output_file = os.path.join(sf_folder, f"query_execution_times_avg_combined.xlsx")

    try:
        # Create a new Excel writer object to save all sheets in one file
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Save each frequency-thread combination as a separate sheet
            for (freq, thread), group_df in summary_df.groupby(['Frequency', 'Thread']):
                # Format sheet name as F{frequency}_T{thread}
                sheet_name = f"F{freq}_T{thread}"  # e.g., "F2.5_T20"

                # Select only the desired columns with updated headers
                group_df[['Query', 'Average Time(s)', 'Sample Std Dev(s)', 'Std Error(s)']].to_excel(
                    writer,
                    index=False,
                    sheet_name=sheet_name
                )

        logging.info(f"Excel file for '{sf.upper()}' saved to '{output_file}'")
    except Exception as e:
        logging.error(f"Failed to write Excel file for '{sf.upper()}': {e}")
