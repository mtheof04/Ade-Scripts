import pandas as pd
import glob
import re
import sys
import os
from datetime import datetime
from statistics import stdev
from math import sqrt

def parse_datetime_from_string(datetime_str):
    """Converts an ISO8601 string to a datetime object."""
    return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

def load_query_timestamps(timestamp_file_path):
    """Loads query start and finish times from a timestamp file."""
    query_times = {}
    with open(timestamp_file_path, 'r') as f:
        for line in f:
            match = re.match(r"Query (\d+): Start Time = (.+Z), Finish Time = (.+Z)", line)
            if match:
                query_id, start, finish = match.groups()
                query_times[int(query_id)] = (parse_datetime_from_string(start), parse_datetime_from_string(finish))
    return query_times

def load_power_data_in_time_range(file_path, start_time, end_time):
    """
    Extracts CPU, DIMM, and Average watt values within a specific time range from an iLO power file,
    organized by sections, including timestamps.
    """
    sections = {"CpuWatts": [[]], "DimmWatts": [[]], "Average": [[]]}
    in_power_detail_section = False
    entry_data = {}

    with open(file_path, 'r') as f:
        for line in f:
            if '"PowerDetail": [' in line:
                in_power_detail_section = True
                continue

            if line.startswith("------------------------------------------------------------------------------------") and in_power_detail_section:
                for key in sections:
                    sections[key].append([])  # Start a new list for each section
                continue

            if in_power_detail_section:
                if line.strip() == "{":
                    entry_data = {}
                    continue

                if line.strip() == "}," or line.strip() == "}":
                    timestamp_str = entry_data.get("Time")
                    if timestamp_str:
                        timestamp = parse_datetime_from_string(timestamp_str)
                        if start_time <= timestamp <= end_time:
                            for key in ["CpuWatts", "DimmWatts", "Average"]:
                                value = entry_data.get(key)
                                if value is not None:
                                    if not sections[key][-1]:  # Initialize if empty
                                        sections[key][-1] = []
                                    sections[key][-1].append((timestamp_str, value))
                    entry_data = {}
                    continue

                match_time = re.search(r'"Time": "(.+Z)"', line)
                match_cpu = re.search(r'"CpuWatts": (\d+)', line)
                match_dimm = re.search(r'"DimmWatts": (\d+)', line)
                match_avg = re.search(r'"Average": (\d+)', line)
                if match_time:
                    entry_data["Time"] = match_time.group(1)
                if match_cpu:
                    entry_data["CpuWatts"] = int(match_cpu.group(1))
                if match_dimm:
                    entry_data["DimmWatts"] = int(match_dimm.group(1))
                if match_avg:
                    entry_data["Average"] = int(match_avg.group(1))

    return sections

def get_last_X_idle_values(idle_file, number, output_file):
    """
    Retrieves the last X idle values from the idle power file and writes output to a file and console.
    """
    idle_values = {"CpuWatts": [], "DimmWatts": [], "Average": []}
    with open(idle_file, 'r') as f:
        lines = f.readlines()

    # Reverse iterate to get the last 'number' entries
    count = 0
    entry_data = {}
    for line in reversed(lines):
        if line.strip() == "}," or line.strip() == "}":
            if "CpuWatts" in entry_data and "DimmWatts" in entry_data and "Average" in entry_data:
                idle_values["CpuWatts"].insert(0, entry_data["CpuWatts"])
                idle_values["DimmWatts"].insert(0, entry_data["DimmWatts"])
                idle_values["Average"].insert(0, entry_data["Average"])
                count += 1
                if count >= number:
                    break
            entry_data = {}
            continue

        match_cpu = re.search(r'"CpuWatts": (\d+)', line)
        match_dimm = re.search(r'"DimmWatts": (\d+)', line)
        match_avg = re.search(r'"Average": (\d+)', line)
        if match_cpu:
            entry_data["CpuWatts"] = int(match_cpu.group(1))
        if match_dimm:
            entry_data["DimmWatts"] = int(match_dimm.group(1))
        if match_avg:
            entry_data["Average"] = int(match_avg.group(1))

    # Write the idle values to the output file and console
    message = f"\nLast {number} Idle Values from {idle_file}:\n"
    write_output(output_file, message)
    for i in range(number):
        if i < len(idle_values["CpuWatts"]):
            message = (f"Idle Value {i+1}: CPU = {idle_values['CpuWatts'][i]} Watts, "
                       f"DIMM = {idle_values['DimmWatts'][i]} Watts, "
                       f"Average = {idle_values['Average'][i]} Watts\n")
            write_output(output_file, message)
        else:
            message = f"Idle Value {i+1}: [No Data]\n"
            write_output(output_file, message)

    return idle_values

def find_sf_folders(base_path):
    """
    Discover all SF folders (e.g., SF100, SF300) within the base_path.
    """
    sf_folders = [f for f in os.listdir(base_path)
                  if re.match(r'^SF\d+$', f) and os.path.isdir(os.path.join(base_path, f))]
    return sf_folders

def write_output(output_file, message):
    """Writes message to both the output file and the console."""
    with open(output_file, 'a') as of:
        of.write(message)
    print(message, end='')  # Use end='' to avoid double newlines

def find_folders_and_process(sf_folder_path, sf_number, idle_number, output_file):
    """
    For a given SF folder, find all F and T folders and process the power data.
    """
    all_dataframes = {}

    frequency_folders = glob.glob(os.path.join(sf_folder_path, "F*"))
    for frequency_folder in frequency_folders:
        freq = os.path.basename(frequency_folder).lower()
        thread_folders = glob.glob(os.path.join(frequency_folder, "T*"))
        for thread_folder in thread_folders:
            threads = os.path.basename(thread_folder).lower()

            timestamp_file_path = os.path.join(thread_folder, 'IloPower', f'query_timestamps_{freq}_sf{sf_number}_{threads}.txt')

            if not os.path.exists(timestamp_file_path):
                continue

            query_times = load_query_timestamps(timestamp_file_path)
            query_metrics = []

            message = f"\nFrequency: {freq}, SF: {sf_number}, Threads: {threads}\n"
            write_output(output_file, message)

            for query_id, (start_time, end_time) in query_times.items():
                power_file_path = os.path.join(thread_folder, 'IloPower', f'ilo_power_{freq}_sf{sf_number}_{threads}.txt')

                if not os.path.exists(power_file_path):
                    continue

                power_values_sections = load_power_data_in_time_range(power_file_path, start_time, end_time)

                # Format start and end times without timezone offset
                start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

                message = f"\nQuery {query_id}: Power values per section within range:\n"
                message += f"Start Time: {start_time_str}\n"
                message += f"End Time: {end_time_str}\n"
                write_output(output_file, message)

                metrics = {}
                for key in ["CpuWatts", "DimmWatts", "Average"]:
                    # Write sections to the output file and console
                    for idx, section in enumerate(power_values_sections[key], start=1):
                        if section:
                            values = [value for timestamp_str, value in section]
                            message = f"{key} Section {idx}: {values}\n"
                            write_output(output_file, message)
                        else:
                            message = f"{key} Section {idx}: [Empty]\n"
                            write_output(output_file, message)

                    # Use the section corresponding to the query number
                    section_index = query_id - 1  # Zero-based index
                    if section_index < len(power_values_sections[key]):
                        selected_section = power_values_sections[key][section_index]
                    else:
                        selected_section = []

                    # Calculate metrics
                    if selected_section:
                        values = [value for timestamp_str, value in selected_section]
                        average_power = sum(values) / len(values)
                        std_dev_power = stdev(values) if len(values) > 1 else 0
                        std_err_power = std_dev_power / sqrt(len(values)) if len(values) > 1 else 0
                    else:
                        average_power = 0
                        std_dev_power = 0
                        std_err_power = 0

                    metrics[f"{key}_Average"] = average_power
                    metrics[f"{key}_StdDev"] = std_dev_power
                    metrics[f"{key}_StdErr"] = std_err_power

                    # Write values with timestamps to the output file and console
                    if selected_section:
                        message = f"\n{key} values for Query {query_id} (uses Section {query_id}):\n"
                        write_output(output_file, message)
                        for timestamp_str, value in selected_section:
                            # Format timestamp without timezone offset
                            timestamp_formatted = timestamp_str.replace('+00:00', 'Z')
                            message = f"{value} at {timestamp_formatted}\n"
                            write_output(output_file, message)
                    else:
                        message = f"No data for {key} for Query {query_id} in Section {query_id}\n"
                        write_output(output_file, message)

                    message = (f"Average {key} Power for Query {query_id}: {average_power:.2f} Watts\n"
                               f"Standard Deviation of {key} Power for Query {query_id}: {std_dev_power:.2f} Watts\n"
                               f"Standard Error of {key} Power for Query {query_id}: {std_err_power:.2f} Watts\n")
                    write_output(output_file, message)

                query_metrics.append({
                    "Query": query_id,
                    "CPU Average Power (Watts)": metrics["CpuWatts_Average"],
                    "CPU Std Dev (Watts)": metrics["CpuWatts_StdDev"],
                    "CPU Std Err (Watts)": metrics["CpuWatts_StdErr"],
                    "DIMM Average Power (Watts)": metrics["DimmWatts_Average"],
                    "DIMM Std Dev (Watts)": metrics["DimmWatts_StdDev"],
                    "DIMM Std Err (Watts)": metrics["DimmWatts_StdErr"],
                    "Overall Average Power (Watts)": metrics["Average_Average"],
                    "Overall Std Dev (Watts)": metrics["Average_StdDev"],
                    "Overall Std Err (Watts)": metrics["Average_StdErr"]
                })

            # Add idle power data
            idle_power_file_path = os.path.join(thread_folder, 'IloPower', f'ilo_power_{freq}_sf{sf_number}_{threads}_idle.txt')
            if os.path.exists(idle_power_file_path):
                idle_last_values = get_last_X_idle_values(idle_power_file_path, idle_number, output_file)
                # Calculate average and std dev for idle values
                cpu_avg_idle = sum(idle_last_values["CpuWatts"]) / len(idle_last_values["CpuWatts"]) if idle_last_values["CpuWatts"] else 0
                dimm_avg_idle = sum(idle_last_values["DimmWatts"]) / len(idle_last_values["DimmWatts"]) if idle_last_values["DimmWatts"] else 0
                avg_avg_idle = sum(idle_last_values["Average"]) / len(idle_last_values["Average"]) if idle_last_values["Average"] else 0

                cpu_std_idle = stdev(idle_last_values["CpuWatts"]) if len(idle_last_values["CpuWatts"]) > 1 else 0
                dimm_std_idle = stdev(idle_last_values["DimmWatts"]) if len(idle_last_values["DimmWatts"]) > 1 else 0
                avg_std_idle = stdev(idle_last_values["Average"]) if len(idle_last_values["Average"]) > 1 else 0

                cpu_std_err_idle = cpu_std_idle / sqrt(len(idle_last_values["CpuWatts"])) if len(idle_last_values["CpuWatts"]) > 1 else 0
                dimm_std_err_idle = dimm_std_idle / sqrt(len(idle_last_values["DimmWatts"])) if len(idle_last_values["DimmWatts"]) > 1 else 0
                avg_std_err_idle = avg_std_idle / sqrt(len(idle_last_values["Average"])) if len(idle_last_values["Average"]) > 1 else 0

                query_metrics.append({
                    "Query": "Idle",
                    "CPU Average Power (Watts)": cpu_avg_idle,
                    "CPU Std Dev (Watts)": cpu_std_idle,
                    "CPU Std Err (Watts)": cpu_std_err_idle,
                    "DIMM Average Power (Watts)": dimm_avg_idle,
                    "DIMM Std Dev (Watts)": dimm_std_idle,
                    "DIMM Std Err (Watts)": dimm_std_err_idle,
                    "Overall Average Power (Watts)": avg_avg_idle,
                    "Overall Std Dev (Watts)": avg_std_idle,
                    "Overall Std Err (Watts)": avg_std_err_idle
                })

            df = pd.DataFrame(query_metrics)
            all_dataframes[f"{freq}_{threads}".upper()] = df

    output_excel_file = os.path.join(sf_folder_path, f"power_metrics_combined.xlsx")
    with pd.ExcelWriter(output_excel_file) as writer:
        for key, df in all_dataframes.items():
            # Ensure sheet names do not exceed Excel's limit of 31 characters
            sheet_name = key.upper()[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def main():
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

    # Manually set the number of idle values you want to retrieve
    idle_number = 20

    # Process each SF folder
    for sf_folder in sf_folders:
        sf_number_match = re.match(r'^SF(\d+)$', sf_folder)
        if sf_number_match:
            sf_number = sf_number_match.group(1)
            sf_folder_path = os.path.join(base_path, sf_folder)
            
            # Define the output file path inside the SF folder
            output_file = os.path.join(sf_folder_path, 'power_metrics_stat.txt')
            
            # Clear the output file if it exists
            open(output_file, 'w').close()

            message = f"\nProcessing SF folder: {sf_folder} (sf={sf_number})\n"
            write_output(output_file, message)
            find_folders_and_process(sf_folder_path, sf_number, idle_number, output_file)
        else:
            message = f"Skipping unrecognized folder: {sf_folder}\n"
            write_output(output_file, message)

    print("\nAll data has been saved to their respective SF folders.")

if __name__ == "__main__":
    main()
