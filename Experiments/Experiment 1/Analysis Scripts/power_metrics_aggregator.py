import os
import re
import glob
import logging
import argparse
from datetime import datetime
import pandas as pd
import math

def configure_logging(log_file_path):
    """
    Configure logging to output to both console and a file.
    """
    logger = logging.getLogger(log_file_path)
    logger.setLevel(logging.INFO)
    
    # Prevent adding multiple handlers to the same logger
    if not logger.handlers:
        # Create handlers
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(log_file_path)
        
        # Create formatters and add to handlers
        c_format = logging.Formatter('%(levelname)s: %(message)s')
        f_format = logging.Formatter('%(levelname)s: %(message)s')
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)
        
        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
    
    return logger

# Constants
TOTAL_SECTIONS = 8  # Total number of sections to print

def extract_section_values(ilo_power_file, start_time, end_time, logger):
    """
    Extract values for CPU, DIMM, and Average from the ilo_power_all.txt file within the given time range.

    Args:
        ilo_power_file (str): Path to the ilo_power_all.txt file.
        start_time (datetime): Start time for filtering entries.
        end_time (datetime): End time for filtering entries.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        list: List of sections, each containing dictionaries with 'Time', 'CpuWatts', 'DimmWatts', and 'Average' lists.
    """
    try:
        with open(ilo_power_file, 'r') as f:
            lines = f.readlines()
        logger.debug(f"Opened file: {ilo_power_file}")
    except FileNotFoundError:
        logger.error(f"File not found: {ilo_power_file}")
        return None
    except Exception as e:
        logger.error(f"Error reading file {ilo_power_file}: {e}")
        return None

    sections = []  # List to hold data for each section
    current_section = {'Time': [], 'CpuWatts': [], 'DimmWatts': [], 'Average': []}
    inside_entry = False
    entry_data = {}
    total_entries = 0
    matched_entries = 0

    for line in lines:
        # Detect section separator
        if line.strip() == "------------------------------------------------------------------------------------":
            sections.append(current_section)
            current_section = {'Time': [], 'CpuWatts': [], 'DimmWatts': [], 'Average': []}
            continue

        if '{' in line:
            entry_data = {}
            inside_entry = True
            total_entries += 1
            continue

        if '}' in line and inside_entry:
            inside_entry = False
            if 'Time' in entry_data:
                try:
                    timestamp = datetime.strptime(entry_data['Time'], "%Y-%m-%dT%H:%M:%SZ")
                except ValueError as e:
                    logger.warning(f"Error parsing time in {ilo_power_file}: {e}")
                    continue
                if start_time <= timestamp <= end_time:
                    matched_entries += 1
                    current_section['Time'].append(entry_data['Time'])
                    if 'CpuWatts' in entry_data:
                        current_section['CpuWatts'].append(entry_data['CpuWatts'])
                    if 'DimmWatts' in entry_data:
                        current_section['DimmWatts'].append(entry_data['DimmWatts'])
                    if 'Average' in entry_data:
                        current_section['Average'].append(entry_data['Average'])
            continue

        if inside_entry:
            # Extract key-value pairs using regex
            time_match = re.search(r'"Time":\s*"([^"]+)"', line)
            cpu_watts_match = re.search(r'"CpuWatts":\s*(\d+)', line)
            dimm_watts_match = re.search(r'"DimmWatts":\s*(\d+)', line)
            average_match = re.search(r'"Average":\s*(\d+)', line)
            if time_match:
                entry_data['Time'] = time_match.group(1)
            if cpu_watts_match:
                entry_data['CpuWatts'] = int(cpu_watts_match.group(1))
            if dimm_watts_match:
                entry_data['DimmWatts'] = int(dimm_watts_match.group(1))
            if average_match:
                entry_data['Average'] = int(average_match.group(1))

    # Append the last section if not already appended
    if any(current_section[key] for key in current_section):
        sections.append(current_section)

    logger.debug(f"Total Entries Processed: {total_entries}")
    logger.debug(f"Entries Matched within Time Range: {matched_entries}")

    # Ensure there are exactly TOTAL_SECTIONS sections
    while len(sections) < TOTAL_SECTIONS:
        sections.append({'Time': [], 'CpuWatts': [], 'DimmWatts': [], 'Average': []})

    # If more than TOTAL_SECTIONS, truncate the list
    if len(sections) > TOTAL_SECTIONS:
        sections = sections[:TOTAL_SECTIONS]

    return sections

def get_last_X_idle_values(idle_file, number, logger):
    """
    Retrieves the last X idle values from the idle power file and logs the output.

    Args:
        idle_file (str): Path to the ilo_power_idle.txt file.
        number (int): Number of idle values to retrieve.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        dict: Dictionary containing the last X idle values for 'CpuWatts', 'DimmWatts', and 'Average'.
    """
    idle_values = {"CpuWatts": [], "DimmWatts": [], "Average": []}
    try:
        with open(idle_file, 'r') as f:
            lines = f.readlines()
        logger.debug(f"Opened idle file: {idle_file}")

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

        # Log the idle values to the console and logger
        logger.info(f"\nLast {number} Idle Values from {idle_file}:")
        for i in range(number):
            if i < len(idle_values["CpuWatts"]):
                logger.info(
                    f"Idle Value {i+1}: CPU = {idle_values['CpuWatts'][i]} Watts, "
                    f"DIMM = {idle_values['DimmWatts'][i]} Watts, "
                    f"Average = {idle_values['Average'][i]} Watts"
                )
            else:
                logger.info(f"Idle Value {i+1}: [No Data]")

    except FileNotFoundError:
        logger.error(f"Idle file not found: {idle_file}")
    except Exception as e:
        logger.error(f"Error reading idle file {idle_file}: {e}")

    return idle_values

def read_timestamps(timestamp_file, logger):
    """
    Read start and end times from ilo_power_timestamps.txt.

    Args:
        timestamp_file (str): Path to the ilo_power_timestamps.txt file.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        tuple: (start_time, end_time, total_execution_time)
            - start_time (datetime): Parsed start time.
            - end_time (datetime): Parsed end time.
            - total_execution_time (int): Total query execution time in seconds.
    """
    start_time = None
    end_time = None
    total_execution_time = None

    try:
        with open(timestamp_file, 'r') as f:
            for line in f:
                # Extract Start Time and End Time
                time_match = re.search(r"Start Time\s*=\s*([^,]+),\s*End Time\s*=\s*([^,]+)", line)
                if time_match:
                    start_time_str, end_time_str = time_match.groups()
                    try:
                        start_time = datetime.strptime(start_time_str.strip(), "%Y-%m-%dT%H:%M:%SZ")
                        end_time = datetime.strptime(end_time_str.strip(), "%Y-%m-%dT%H:%M:%SZ")
                        logger.debug(f"Parsed timestamps from {timestamp_file}: Start Time = {start_time}, End Time = {end_time}")
                    except ValueError as e:
                        logger.error(f"Error parsing timestamps in {timestamp_file}: {e}")
                # Extract Total Query Execution Time
                exec_time_match = re.search(r"Total Query Execution Time:\s*(\d+)\s*seconds", line)
                if exec_time_match:
                    total_execution_time = int(exec_time_match.group(1))
                    logger.debug(f"Parsed total execution time from {timestamp_file}: {total_execution_time} seconds")
    except FileNotFoundError:
        logger.error(f"Timestamp file not found: {timestamp_file}")
    except Exception as e:
        logger.error(f"Error reading timestamp file {timestamp_file}: {e}")

    return start_time, end_time, total_execution_time

def process_iteration(ilo_power_file, iteration_dir, iteration_number, logger):
    """
    Process a single iteration by reading timestamps and extracting power data.

    Args:
        ilo_power_file (str): Path to the shared ilo_power_all.txt file.
        iteration_dir (str): Path to the current Iteration* directory.
        iteration_number (int): The current iteration number.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        dict: Dictionary containing computed statistics for the iteration and raw data.
    """
    timestamp_file = os.path.join(iteration_dir, "ilo_power_timestamps.txt")

    # Read timestamps
    start_time, end_time, total_exec_time = read_timestamps(timestamp_file, logger)

    if not start_time or not end_time:
        logger.warning(f"  [Iteration {iteration_number}] Could not read valid timestamps from {timestamp_file}. Skipping this iteration.")
        return None

    # Process 'ilo_power_all.txt' with the extracted timestamps
    sections = extract_section_values(ilo_power_file, start_time, end_time, logger)

    if sections is None:
        logger.warning(f"  [Iteration {iteration_number}] Skipping due to previous errors.")
        return None

    # Print extracted values for all sections
    logger.info(f"\n[Iteration {iteration_number}] Extracted Values from {ilo_power_file}:")
    for idx, section in enumerate(sections, start=1):
        logger.info(f"  Section {idx}:")
        logger.info(f"    CPU Watts: {section['CpuWatts']}")
        logger.info(f"    DIMM Watts: {section['DimmWatts']}")
        logger.info(f"    Average: {section['Average']}")

    # Map Iteration N to Section N
    section_index = iteration_number - 1
    if section_index >= len(sections):
        logger.warning(f"  [Iteration {iteration_number}] No corresponding section found in the data.")
        return None

    current_section = sections[section_index]

    # Print Start and End Timestamps
    logger.info(f"\nIteration {iteration_number} uses Section {iteration_number}")
    logger.info(f"  Start Time: {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    logger.info(f"  End Time: {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    # For CPU Watts
    logger.info("  For CPU Watts:")
    for time_str, cpu_watt in zip(current_section['Time'], current_section['CpuWatts']):
        logger.info(f"    {cpu_watt} at {time_str}")

    # For DIMM Watts
    logger.info("  For DIMM Watts:")
    for time_str, dimm_watt in zip(current_section['Time'], current_section['DimmWatts']):
        logger.info(f"    {dimm_watt} at {time_str}")

    # For Average
    logger.info("  For Average:")
    for time_str, avg_power in zip(current_section['Time'], current_section['Average']):
        logger.info(f"    {avg_power} at {time_str}")

    # Aggregate metrics for this iteration
    iteration_metrics = {
        'CpuWatts': current_section['CpuWatts'],
        'DimmWatts': current_section['DimmWatts'],
        'AveragePower': current_section['Average'],
    }

    # Compute statistics for this iteration
    aggregate_metrics = compute_statistics(
        iteration_metrics['CpuWatts'],
        iteration_metrics['DimmWatts'],
        iteration_metrics['AveragePower'],
        phase=f'Iteration {iteration_number}'
    )

    # Print average values
    logger.info(f"  [Iteration {iteration_number}] Average CPU Watts: {aggregate_metrics['Average CPU Watts']}")
    logger.info(f"  [Iteration {iteration_number}] Average DIMM Watts: {aggregate_metrics['Average DIMM Watts']}")
    logger.info(f"  [Iteration {iteration_number}] Average Power: {aggregate_metrics['Average Power']}")

    return {
        'Phase': f'Iteration {iteration_number}',
        'Average CPU Watts': aggregate_metrics['Average CPU Watts'],
        'CPU Watts Sample StdDev': aggregate_metrics['CPU Watts Sample StdDev'],
        'CPU Watts StdError': aggregate_metrics['CPU Watts StdError'],
        'Average DIMM Watts': aggregate_metrics['Average DIMM Watts'],
        'DIMM Watts Sample StdDev': aggregate_metrics['DIMM Watts Sample StdDev'],
        'DIMM Watts StdError': aggregate_metrics['DIMM Watts StdError'],
        'Average Power': aggregate_metrics['Average Power'],
        'Power Sample StdDev': aggregate_metrics['Power Sample StdDev'],
        'Power StdError': aggregate_metrics['Power StdError'],
        'CpuWatts': iteration_metrics['CpuWatts'],
        'DimmWatts': iteration_metrics['DimmWatts'],
        'AveragePowerValues': iteration_metrics['AveragePower']
    }

def get_last_X_idle_values(idle_file, number, logger):
    """
    Retrieves the last X idle values from the idle power file and logs the output.

    Args:
        idle_file (str): Path to the ilo_power_idle.txt file.
        number (int): Number of idle values to retrieve.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        dict: Dictionary containing the last X idle values for 'CpuWatts', 'DimmWatts', and 'Average'.
    """
    idle_values = {"CpuWatts": [], "DimmWatts": [], "Average": []}
    try:
        with open(idle_file, 'r') as f:
            lines = f.readlines()
        logger.debug(f"Opened idle file: {idle_file}")

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

        # Log the idle values to the console and logger
        logger.info(f"\nLast {number} Idle Values from {idle_file}:")
        for i in range(number):
            if i < len(idle_values["CpuWatts"]):
                logger.info(
                    f"Idle Value {i+1}: CPU = {idle_values['CpuWatts'][i]} Watts, "
                    f"DIMM = {idle_values['DimmWatts'][i]} Watts, "
                    f"Average = {idle_values['Average'][i]} Watts"
                )
            else:
                logger.info(f"Idle Value {i+1}: [No Data]")

    except FileNotFoundError:
        logger.error(f"Idle file not found: {idle_file}")
    except Exception as e:
        logger.error(f"Error reading idle file {idle_file}: {e}")

    return idle_values

def read_last_x_idle_watts(idle_file_path, num_idle_values, logger):
    """
    Read the last X idle watt values for CPU, DIMM, and Average from ilo_power_idle.txt.

    Args:
        idle_file_path (str): Path to the ilo_power_idle.txt file.
        num_idle_values (int): Number of idle values to extract.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        dict: Dictionary containing lists of the last X values for 'CpuWatts', 'DimmWatts', and 'Average'.
              Returns empty lists if not found or error.
    """
    return get_last_X_idle_values(idle_file_path, num_idle_values, logger)

def process_target_directory(target_dir, logger):
    """
    Process all iterations within a target directory and collect per-iteration metrics.

    Args:
        target_dir (str): Path to the target Csv or Parquet directory.
        logger (logging.Logger): Logger instance for logging messages.

    Returns:
        pandas.DataFrame: DataFrame containing per-iteration metrics, Aggregate, and Idle metrics.
    """
    logger.info(f"\nProcessing Target Directory: {target_dir}")

    # Path to 'ilo_power_all.txt' in the target directory
    ilo_power_file = os.path.join(target_dir, "ilo_power_all.txt")

    if not os.path.exists(ilo_power_file):
        logger.error(f"  'ilo_power_all.txt' not found in {target_dir}. Skipping this directory.")
        return None

    # Find all Iteration* subdirectories within the target directory
    iteration_pattern = os.path.join(target_dir, 'Iteration*')
    iteration_dirs = sorted(glob.glob(iteration_pattern))

    # Initialize lists to collect per-iteration metrics and raw data
    iteration_metrics_list = []
    all_cpu_watts = []
    all_dimm_watts = []
    all_average_power = []

    if not iteration_dirs:
        logger.warning(f"  No Iteration directories found in {target_dir}.")
    else:
        logger.info(f"  Found {len(iteration_dirs)} Iteration directories:")
        for iter_idx, iter_dir in enumerate(iteration_dirs, start=1):
            logger.info(f"    [{iter_idx}] {iter_dir}")

            # Process each iteration
            result = process_iteration(ilo_power_file, iter_dir, iter_idx, logger)
            if result:
                # Extract metrics and raw data
                iteration_metrics = {
                    'Phase': result['Phase'],
                    'Average CPU Watts': result['Average CPU Watts'],
                    'CPU Watts Sample StdDev': result['CPU Watts Sample StdDev'],
                    'CPU Watts StdError': result['CPU Watts StdError'],
                    'Average DIMM Watts': result['Average DIMM Watts'],
                    'DIMM Watts Sample StdDev': result['DIMM Watts Sample StdDev'],
                    'DIMM Watts StdError': result['DIMM Watts StdError'],
                    'Average Power': result['Average Power'],
                    'Power Sample StdDev': result['Power Sample StdDev'],
                    'Power StdError': result['Power StdError']
                }
                iteration_metrics_list.append(iteration_metrics)

                # Collect raw data
                all_cpu_watts.extend(result['CpuWatts'])
                all_dimm_watts.extend(result['DimmWatts'])
                all_average_power.extend(result['AveragePowerValues'])

                # After each iteration, save the current state of the tables
                logger.info(f"\n--- After Iteration {iter_idx} ---")
                logger.info(f"CPU Watts Table: {all_cpu_watts}")
                logger.info(f"DIMM Watts Table: {all_dimm_watts}")
                logger.info(f"Average Power Table: {all_average_power}")
                logger.info(f"---------------------------\n")

    if not iteration_metrics_list:
        logger.warning(f"No iteration metrics collected in {target_dir}.")
        return None

    # Create DataFrame from iteration_metrics_list
    df_iterations = pd.DataFrame(iteration_metrics_list)

    # Reorder columns to have 'Phase' first
    columns_order = ['Phase', 'Average CPU Watts', 'CPU Watts Sample StdDev', 'CPU Watts StdError',
                     'Average DIMM Watts', 'DIMM Watts Sample StdDev', 'DIMM Watts StdError',
                     'Average Power', 'Power Sample StdDev', 'Power StdError']
    df_iterations = df_iterations[columns_order]

    # Print the tables with all collected values
    logger.info(f"\n=== Final Tables for {target_dir} ===")
    logger.info(f"CPU Watts Table: {all_cpu_watts}")
    logger.info(f"DIMM Watts Table: {all_dimm_watts}")
    logger.info(f"Average Power Table: {all_average_power}")
    logger.info("====================================\n")

    # Compute Aggregate Metrics from all iterations
    aggregate_metrics = compute_statistics(
        all_cpu_watts,
        all_dimm_watts,
        all_average_power,
        phase='Aggregate'
    )

    if aggregate_metrics:
        # Add 'Phase' key
        aggregate_metrics_ordered = {
            'Phase': 'Aggregate',
            'Average CPU Watts': aggregate_metrics['Average CPU Watts'],
            'CPU Watts Sample StdDev': aggregate_metrics['CPU Watts Sample StdDev'],
            'CPU Watts StdError': aggregate_metrics['CPU Watts StdError'],
            'Average DIMM Watts': aggregate_metrics['Average DIMM Watts'],
            'DIMM Watts Sample StdDev': aggregate_metrics['DIMM Watts Sample StdDev'],
            'DIMM Watts StdError': aggregate_metrics['DIMM Watts StdError'],
            'Average Power': aggregate_metrics['Average Power'],
            'Power Sample StdDev': aggregate_metrics['Power Sample StdDev'],
            'Power StdError': aggregate_metrics['Power StdError']
        }

        # Append aggregate_metrics to df_iterations
        df_aggregate = pd.DataFrame([aggregate_metrics_ordered])
        df_iterations = pd.concat([df_iterations, df_aggregate], ignore_index=True)

    # Read Idle Metrics
    idle_file = os.path.join(target_dir, "ilo_power_idle.txt")
    last_X_idle = get_last_X_idle_values(idle_file, 20, logger)

    # Compute Idle Metrics
    idle_metrics = compute_statistics(
        last_X_idle['CpuWatts'],
        last_X_idle['DimmWatts'],
        last_X_idle['Average'],
        phase='Idle'
    )

    if idle_metrics:
        # Add 'Phase' key
        idle_metrics_ordered = {
            'Phase': 'Idle',
            'Average CPU Watts': idle_metrics['Average CPU Watts'],
            'CPU Watts Sample StdDev': idle_metrics['CPU Watts Sample StdDev'],
            'CPU Watts StdError': idle_metrics['CPU Watts StdError'],
            'Average DIMM Watts': idle_metrics['Average DIMM Watts'],
            'DIMM Watts Sample StdDev': idle_metrics['DIMM Watts Sample StdDev'],
            'DIMM Watts StdError': idle_metrics['DIMM Watts StdError'],
            'Average Power': idle_metrics['Average Power'],
            'Power Sample StdDev': idle_metrics['Power Sample StdDev'],
            'Power StdError': idle_metrics['Power StdError']
        }

        # Append idle_metrics to df_iterations
        df_idle = pd.DataFrame([idle_metrics_ordered])
        df_iterations = pd.concat([df_iterations, df_idle], ignore_index=True)

        # Log the last 40 idle values
        logger.info(f"Last Idle CPU Watts: {last_X_idle['CpuWatts']}")
        logger.info(f"Last Idle DIMM Watts: {last_X_idle['DimmWatts']}")
        logger.info(f"Last Idle Average Power: {last_X_idle['Average']}")

    return df_iterations

def compute_statistics(cpu_watts, dimm_watts, average_power, phase='Aggregate'):
    """
    Compute average, sample standard deviation, and standard error for CPU, DIMM, and Power metrics.

    Args:
        cpu_watts (list): List of CPU watt values.
        dimm_watts (list): List of DIMM watt values.
        average_power (list): List of Average power values.
        phase (str): Phase name ('Aggregate', 'Idle', or 'Iteration N').

    Returns:
        dict: Dictionary containing computed statistics.
    """
    metrics = {}

    # Helper function to calculate statistics
    def calc_stats(data):
        if not data:
            return {'Average': None, 'StdDev': None, 'StdError': None}
        avg = sum(data) / len(data)
        if len(data) > 1:
            variance = sum((x - avg) ** 2 for x in data) / (len(data) - 1)
            stddev = math.sqrt(variance)
            stderr = stddev / math.sqrt(len(data))
        else:
            stddev = None
            stderr = None
        return {'Average': avg, 'StdDev': stddev, 'StdError': stderr}

    # Calculate statistics for CPU Watts
    cpu_stats = calc_stats(cpu_watts)
    metrics['Average CPU Watts'] = cpu_stats['Average']
    metrics['CPU Watts Sample StdDev'] = cpu_stats['StdDev']
    metrics['CPU Watts StdError'] = cpu_stats['StdError']

    # Calculate statistics for DIMM Watts
    dimm_stats = calc_stats(dimm_watts)
    metrics['Average DIMM Watts'] = dimm_stats['Average']
    metrics['DIMM Watts Sample StdDev'] = dimm_stats['StdDev']
    metrics['DIMM Watts StdError'] = dimm_stats['StdError']

    # Calculate statistics for Average Power
    power_stats = calc_stats(average_power)
    metrics['Average Power'] = power_stats['Average']
    metrics['Power Sample StdDev'] = power_stats['StdDev']
    metrics['Power StdError'] = power_stats['StdError']

    return metrics

def find_sf_folders(base_path):
    """
    Find all SF* folders within the base path.

    Args:
        base_path (str): The base directory path to start searching from.

    Returns:
        dict: Dictionary mapping SF folder names to their paths.
    """
    sf_pattern = os.path.join(base_path, 'SF*')
    sf_dirs = glob.glob(sf_pattern)
    sf_folders = {os.path.basename(sf_dir): sf_dir for sf_dir in sf_dirs if os.path.isdir(sf_dir)}
    return sf_folders

def find_f_subfolders(sf_dir):
    """
    Find all F* folders within an SF* directory.

    Args:
        sf_dir (str): Path to the SF* directory.

    Returns:
        list: List of F* directory paths.
    """
    f_pattern = os.path.join(sf_dir, 'F*')
    f_dirs = sorted(glob.glob(f_pattern))
    return [f_dir for f_dir in f_dirs if os.path.isdir(f_dir)]

def find_subfolders(f_dir):
    """
    Find 'Csv' and 'Parquet' subdirectories within an F* directory.

    Args:
        f_dir (str): Path to the F* directory.

    Returns:
        list: List of 'Csv' and 'Parquet' directory paths.
    """
    subfolders = ['Csv', 'Parquet']
    subfolder_paths = []
    for subfolder in subfolders:
        path = os.path.join(f_dir, subfolder)
        if os.path.isdir(path):
            subfolder_paths.append(path)
    return subfolder_paths

def process_sf_folder(sf_name, sf_dir):
    """
    Process all F* directories within an SF* folder and compile metrics.

    Args:
        sf_name (str): Name of the SF folder (e.g., 'SF1').
        sf_dir (str): Path to the SF folder.

    Returns:
        dict: Dictionary mapping sheet names to their metrics DataFrames.
    """
    # Define log file path inside the SF folder
    log_file_path = os.path.join(sf_dir, "power_metrics_stats.txt")
    
    # Configure a separate logger for this SF folder
    logger = configure_logging(log_file_path)
    
    logger.info(f"Starting processing for {sf_name} in directory {sf_dir}")

    f_dirs = find_f_subfolders(sf_dir)
    sf_metrics = {}

    for f_dir in f_dirs:
        f_name = os.path.basename(f_dir)  # e.g., 'F1.0'
        subfolders = find_subfolders(f_dir)

        for subfolder_path in subfolders:
            subfolder_name = os.path.basename(subfolder_path)  # 'Csv' or 'Parquet'
            sheet_name = f"{f_name}_{subfolder_name}"

            logger.info(f"Processing {sheet_name} in {sf_name}")

            metrics_df = process_target_directory(subfolder_path, logger)

            if metrics_df is not None and not metrics_df.empty:
                sf_metrics[sheet_name] = metrics_df
            else:
                logger.warning(f"No metrics found for {sheet_name} in {sf_name}")

    logger.info(f"Completed processing for {sf_name}")

    return sf_metrics

def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Process ilo_power data across multiple iterations and save metrics to Excel files.")
    parser.add_argument('base_path', type=str, help='Base directory path to start searching from (e.g., Measures/Files)')
    args = parser.parse_args()

    base_path = os.path.abspath(args.base_path)

    # Find all SF* folders
    sf_folders = find_sf_folders(base_path)

    if not sf_folders:
        print("No SF* directories found in the base path.")
        return
    else:
        print(f"Found {len(sf_folders)} SF* directories:")
        for idx, (sf_name, sf_dir) in enumerate(sf_folders.items(), start=1):
            print(f"  {idx}. {sf_name} at {sf_dir}")

    # Process each SF* folder
    for sf_name, sf_dir in sf_folders.items():
        print(f"\nProcessing SF Folder: {sf_name}")
        sf_metrics = process_sf_folder(sf_name, sf_dir)

        if not sf_metrics:
            print(f"No metrics collected for {sf_name}. Skipping Excel file creation.")
            continue

        # Define Excel file name
        excel_file_name = f"power_metrics_{sf_name}_combined.xlsx"
        excel_file_path = os.path.join(sf_dir, excel_file_name)

        # Write to Excel with multiple sheets
        try:
            with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                for sheet_name, df in sf_metrics.items():
                    # Ensure sheet name is within Excel's limit of 31 characters
                    sheet_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Excel file created: {excel_file_path}")
        except Exception as e:
            print(f"Error writing Excel file {excel_file_path}: {e}")

if __name__ == "__main__":
    main()
