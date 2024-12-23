import os
import re
import sys
import argparse
import numpy as np
import pandas as pd
import logging
from datetime import datetime, timezone

def setup_logger(sf_folder_path):
    """
    Sets up a logger that outputs to both the console and a log file within the SF folder.
    
    Args:
        sf_folder_path (str): Path to the SF folder where the log file will be saved.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create a logger with a unique name based on the SF folder
    sf_folder_name = os.path.basename(sf_folder_path)
    logger = logging.getLogger(sf_folder_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent messages from being propagated to the root logger

    # Check if the logger already has handlers to prevent duplicate logs
    if not logger.handlers:
        # Create handlers
        c_handler = logging.StreamHandler(sys.stdout)
        log_file_path = os.path.join(sf_folder_path, 'power_metrics_stat.txt')
        f_handler = logging.FileHandler(log_file_path, mode='w')
        c_handler.setLevel(logging.INFO)
        f_handler.setLevel(logging.INFO)
        
        # Create formatters and add them to handlers
        formatter = logging.Formatter('%(message)s')
        c_handler.setFormatter(formatter)
        f_handler.setFormatter(formatter)
        
        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    
    return logger

def calculate_sample_std(values):
    """Calculates the sample standard deviation for a list of values."""
    return np.std(values, ddof=1) if len(values) > 1 else None

def calculate_std_error(values):
    """Calculates the standard error for a list of values."""
    return calculate_sample_std(values) / np.sqrt(len(values)) if len(values) > 1 else None

def process_f_folder(f_folder, sf_folder_name, summary_data, logger, print_all_sections=True):
    """
    Processes a single F folder to extract power metrics for each query phase and retrieve idle data.
    
    Args:
        f_folder (str): Path to the F folder.
        sf_folder_name (str): Name of the parent SF folder.
        summary_data (dict): Dictionary to store the summary data.
        logger (logging.Logger): Logger instance for logging messages.
        print_all_sections (bool): Flag to print all sections or only non-empty ones.
    """
    f_folder_name = os.path.basename(f_folder)
    phase_data_list = []

    power_file = os.path.join(f_folder, 'ilo_power_all.txt')
    timestamps_file = os.path.join(f_folder, 'ilo_power_timestamps_all.txt')
    
    if not os.path.isfile(power_file) or not os.path.isfile(timestamps_file):
        logger.info(f"Missing files in {f_folder}. Skipping this folder.")
        return
    
    # Define the timestamp ranges for each query phase by reading from ilo_power_timestamps_all.txt
    timestamp_ranges = parse_timestamp_ranges(timestamps_file, logger)
    logger.info(f"Timestamp ranges in {f_folder}: {timestamp_ranges}")
    
    # Read power metrics from ilo_power_all.txt and extract data per phase
    for phase, (start_time, end_time) in timestamp_ranges.items():
        # Format start_time and end_time with 'Z' suffix
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"\nProcessing phase {phase.upper()} from {start_time_str} to {end_time_str} in {f_folder}")
        metrics = load_power_data_in_time_range(power_file, start_time, end_time, logger)
        
        # Display metrics for each section and the most populated section's stats
        display_metrics_with_std(f_folder, phase, metrics, logger, print_all_sections)
        
        # Extract average, sample std dev, and std error for each metric for this phase to save to Excel
        phase_data = extract_phase_metrics(phase, metrics)
        
        # Add idle data for each phase
        idle_data = process_idle_data(f_folder, phase, logger, X=20)
        if idle_data:
            phase_data.update(idle_data)  # Merge idle data into phase data

        phase_data_list.append(phase_data)
    
    # Organize data under the corresponding SF folder
    if sf_folder_name not in summary_data:
        summary_data[sf_folder_name] = {}
    
    summary_data[sf_folder_name][f_folder_name] = phase_data_list

def parse_timestamp_ranges(timestamps_file, logger):
    """Parses the timestamp ranges from the timestamp file for each query phase."""
    timestamp_ranges = {}
    with open(timestamps_file, 'r') as file:
        phase = None
        for line in file:
            phase_match = re.match(r'(\w+):', line)
            if phase_match:
                phase = phase_match.group(1)
                continue
            
            time_match = re.match(r'Start Time = ([\dT:Z\-]+), End Time = ([\dT:Z\-]+)', line)
            if time_match and phase:
                try:
                    start_time = datetime.fromisoformat(time_match.group(1).replace("Z", "+00:00"))
                    end_time = datetime.fromisoformat(time_match.group(2).replace("Z", "+00:00"))
                    timestamp_ranges[phase] = (start_time, end_time)
                except ValueError:
                    logger.info(f"Invalid timestamp format in {timestamps_file}: {line.strip()}")
                phase = None

    logger.info(f"Parsed timestamp ranges: {timestamp_ranges}")
    return timestamp_ranges

def load_power_data_in_time_range(file_path, start_time, end_time, logger):
    """
    Extracts CPU, DIMM, and Average watt values within a specific time range from an iLO power file, organized by sections.
    
    Args:
        file_path (str): Path to the ilo_power_all.txt file.
        start_time (datetime): Start of the time range.
        end_time (datetime): End of the time range.
        logger (logging.Logger): Logger instance for logging messages.
        
    Returns:
        dict: Dictionary with metrics as keys and lists of sections containing (value, timestamp) tuples.
    """
    sections = {"CpuWatts": [[]], "DimmWatts": [[]], "Average": [[]]}
    in_power_detail_section = False
    entry_data = {}

    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
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
                    timestamp = entry_data.get("Time")
                    if timestamp:
                        try:
                            timestamp_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        except ValueError:
                            logger.info(f"Invalid timestamp format at line {line_num} in {file_path}. Skipping entry.")
                            entry_data = {}
                            continue
                        if start_time <= timestamp_dt <= end_time:
                            for key in ["CpuWatts", "DimmWatts", "Average"]:
                                value = entry_data.get(key)
                                if value is not None:
                                    if not sections[key][-1]:  # Initialize if empty
                                        sections[key][-1] = []
                                    # Format timestamp with 'Z' suffix
                                    timestamp_formatted = timestamp_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                                    sections[key][-1].append((value, timestamp_formatted))
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

def display_metrics_with_std(f_folder, phase, metrics, logger, print_all_sections=True):
    """
    Displays all sections for each metric and calculates statistics on the most populated section.
    
    Args:
        f_folder (str): Path to the F folder.
        phase (str): The current phase being processed.
        metrics (dict): Dictionary containing metric data sections.
        logger (logging.Logger): Logger instance for logging messages.
        print_all_sections (bool): Flag to print all sections or only non-empty ones.
    """
    logger.info(f"\nMetrics for {phase.upper()} in folder {f_folder}:")
    for metric, sections in metrics.items():
        logger.info(f"\n{metric}:")
        for i, section in enumerate(sections, start=1):
            if print_all_sections:
                if section:
                    logger.info(f"  Section {i}:")
                    for value, timestamp in section:
                        logger.info(f"    {value} at {timestamp}")
                else:
                    logger.info(f"  Section {i}: Empty")
            else:
                if section:
                    logger.info(f"  Section {i}:")
                    for value, timestamp in section:
                        logger.info(f"    {value} at {timestamp}")

        # Identify the most populated section for statistical calculations
        non_empty_sections = [section for section in sections if section]
        most_populated_section = max(non_empty_sections, key=len) if non_empty_sections else []

        if most_populated_section:
            # Extract values for statistical calculations
            values = [value for value, _ in most_populated_section]
            # Calculate statistical metrics
            average = np.mean(values) if values else None
            sample_std = calculate_sample_std(values)
            std_error = calculate_std_error(values)
            
            logger.info(f"\n  Most populated section stats for {metric}:")
            logger.info(f"    Average: {average}")
            logger.info(f"    Sample Std Dev: {sample_std}")
            logger.info(f"    Standard Error: {std_error}")
        else:
            logger.info(f"\n  No data available for statistical calculations in {metric}.")

def extract_phase_metrics(phase, metrics):
    """Extracts average, sample standard deviation, and standard error for each metric in a phase, flattening nested lists."""
    phase_data = {
        "Phase": phase.capitalize(),
    }

    # Flatten the lists for each metric
    for metric, sections in metrics.items():
        flat_values = [item[0] for section in sections for item in section if section]  # Extract only values

        if metric == "CpuWatts":
            phase_data["Average CPU Watts"] = np.mean(flat_values) if flat_values else None
            phase_data["CPU Watts Sample Std Dev"] = calculate_sample_std(flat_values) if flat_values else None
            phase_data["CPU Watts Std Error"] = calculate_std_error(flat_values) if flat_values else None
        elif metric == "DimmWatts":
            phase_data["Average DIMM Watts"] = np.mean(flat_values) if flat_values else None
            phase_data["DIMM Watts Sample Std Dev"] = calculate_sample_std(flat_values) if flat_values else None
            phase_data["DIMM Watts Std Error"] = calculate_std_error(flat_values) if flat_values else None
        elif metric == "Average":
            phase_data["Average Power"] = np.mean(flat_values) if flat_values else None
            phase_data["Power Sample Std Dev"] = calculate_sample_std(flat_values) if flat_values else None
            phase_data["Power Std Error"] = calculate_std_error(flat_values) if flat_values else None

    return phase_data

def process_idle_data(f_folder, phase, logger, X=10):
    """
    Processes idle data for the last X entries in each phase folder and prints the average and std deviation.
    
    Args:
        f_folder (str): Path to the F folder.
        phase (str): The current phase being processed.
        logger (logging.Logger): Logger instance for logging messages.
        X (int): Number of last entries to consider for idle data.
        
    Returns:
        dict: Dictionary containing idle data averages and standard errors.
    """
    iteration_path = os.path.join(f_folder, 'Iterations', 'Iteration1', phase)
    if not os.path.isdir(iteration_path):
        logger.info(f"No Iteration1 folder found in {f_folder}/{phase}. Skipping idle data.")
        return {}
    
    idle_file = os.path.join(iteration_path, 'ilo_power_idle.txt')
    if not os.path.isfile(idle_file):
        logger.info(f"No ilo_power_idle.txt found in {iteration_path}. Skipping.")
        return {}

    # Load last X entries
    metrics = load_last_metrics_from_idle_file(idle_file, X)
    
    # Print idle data summary
    logger.info(f"\nIdle data from last {X} entries in {iteration_path}:")
    for metric, values in metrics.items():
        logger.info(f"  {metric}: {values}")
    
    # Calculate statistical metrics
    idle_data = {}
    for metric, values in metrics.items():
        avg_value = np.mean(values) if values else None
        sample_std = calculate_sample_std(values) if len(values) > 1 else None
        std_error = calculate_std_error(values) if len(values) > 1 else None
        
        logger.info(f"  {metric} Statistics:")
        logger.info(f"    Average: {avg_value}")
        logger.info(f"    Sample Std Dev: {sample_std}")
        logger.info(f"    Standard Error: {std_error}")
        
        # Construct the idle data dictionary with new metrics
        idle_data[f"Idle {metric} Average"] = avg_value
        idle_data[f"Idle {metric} Sample Std Dev"] = sample_std
        idle_data[f"Idle {metric} Std Error"] = std_error

    return idle_data

def load_last_metrics_from_idle_file(file_path, X):
    """Loads the last X values for CpuWatts, DimmWatts, and Average from the idle file."""
    cpu_watts, dimm_watts, average = [], [], []
    with open(file_path, 'r') as file:
        for line in file:
            match_cpu = re.search(r'"CpuWatts": (\d+)', line)
            match_dimm = re.search(r'"DimmWatts": (\d+)', line)
            match_avg = re.search(r'"Average": (\d+)', line)
            
            if match_cpu:
                cpu_watts.append(int(match_cpu.group(1)))
            if match_dimm:
                dimm_watts.append(int(match_dimm.group(1)))
            if match_avg:
                average.append(int(match_avg.group(1)))

    return {
        "CpuWatts": cpu_watts[-X:],
        "DimmWatts": dimm_watts[-X:],
        "Average": average[-X:]
    }

def save_to_excel(summary_data, output_file_path, logger):
    """Saves the collected data to an Excel file with separate sheets for each F folder."""
    try:
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            for f_folder, phase_data in summary_data.items():
                df = pd.DataFrame(phase_data)
                # Sheet names in Excel have a maximum length of 31 characters
                sheet_name = f_folder[:31]
                try:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    logger.info(f"Error writing sheet {sheet_name}: {e}")
        logger.info(f"Data saved to {output_file_path}")
    except Exception as e:
        logger.info(f"Failed to save Excel file {output_file_path}: {e}")

def process_all_folders(base_path, print_all_sections=True):
    """
    Processes all SF folders and their respective F folders within the base path and collects data for Excel output.
    
    Args:
        base_path (str): The base directory containing SF folders.
        print_all_sections (bool): Flag to print all sections or only non-empty ones.
    """
    summary_data = {}
    # Identify all SF folders (e.g., SF100, SF300) dynamically
    sf_folders = [f.path for f in os.scandir(base_path) if f.is_dir() and re.match(r'^SF\d+', f.name, re.IGNORECASE)]
    if not sf_folders:
        print("No SF folders found in the base path.")
        return

    for sf_folder in sf_folders:
        # Set up a separate logger for each SF folder
        logger = setup_logger(sf_folder)
        sf_folder_name = os.path.basename(sf_folder)
        logger.info(f"\nProcessing SF folder: {sf_folder_name}")
        # Identify all F folders within the current SF folder
        f_folders = [f.path for f in os.scandir(sf_folder) if f.is_dir() and f.name.lower().startswith('f')]
        if not f_folders:
            logger.info(f"No F folders found in {sf_folder}. Skipping this SF folder.")
            continue

        for f_folder in f_folders:
            process_f_folder(f_folder, sf_folder_name, summary_data, logger, print_all_sections)

        if summary_data.get(sf_folder_name):
            # Save data per SF folder
            output_file_path = os.path.join(base_path, sf_folder_name, f"power_metrics_combined.xlsx")
            save_to_excel(summary_data[sf_folder_name], output_file_path, logger)
        else:
            logger.info(f"No data collected for {sf_folder_name} to save.")

def main():
    parser = argparse.ArgumentParser(description="Process power metrics data.")
    parser.add_argument('base_path', nargs='?', default="Queries/Files", help='Base directory containing SF folders')
    args = parser.parse_args()

    print("Script started.")
    base_path = args.base_path
    print_all_sections = True  # Always print all sections

    if not os.path.isdir(base_path):
        print(f"Base path is not a directory or does not exist: {base_path}")
        sys.exit(1)

    process_all_folders(base_path, print_all_sections=print_all_sections)
    print("Processing completed.")

if __name__ == "__main__":
    main()
