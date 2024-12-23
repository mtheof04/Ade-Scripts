import glob
import re
import os
import sys

def find_sf_folders(base_path):
    """
    Discover all SF folders (e.g., SF100, SF300) within the base_path.
    """
    sf_folders = [f for f in os.listdir(base_path) 
                 if re.match(r'^SF\d+$', f) and os.path.isdir(os.path.join(base_path, f))]
    print(f"Discovered SF folders: {sf_folders}")
    return sf_folders

def find_frequency_and_threads(sf_folder_path, sf_number):
    """
    For a given SF folder, find all frequency and thread folders and process them.
    """
    # Find all frequency folders (e.g., F2.0, F2.5) in the SF folder
    frequency_folders = [f for f in os.listdir(sf_folder_path) 
                         if re.match(r'^F\d+\.\d+$', f) and os.path.isdir(os.path.join(sf_folder_path, f))]
    print(f"Found frequency folders in {sf_folder_path}: {frequency_folders}")
    
    for frequency in frequency_folders:
        frequency_value = frequency[1:]  # Remove the 'F' prefix, e.g., "2.0" instead of "F2.0"
        threads_path = os.path.join(sf_folder_path, frequency)
        
        # Find thread folders (e.g., T20, T40) within the frequency folder
        thread_folders = [t for t in os.listdir(threads_path) 
                          if re.match(r'^T\d+$', t) and os.path.isdir(os.path.join(threads_path, t))]
        print(f"Found thread folders in {frequency}: {thread_folders}")
        
        for threads in thread_folders:
            threads_number = threads[1:]  # Remove the 'T' prefix
            frequency_folder_path = os.path.join(threads_path, threads)
            process_all_queries(frequency_folder_path, sf_number, frequency_value, threads_number)

def process_all_queries(base_path, sf, frequency_value, threads_number):
    """
    Process all query files matching the pattern within the given base_path.
    """
    # Define file path pattern
    file_path_pattern = os.path.join(base_path, 'Iostat', 'Aggregated', 
                                     f'iostat_f{frequency_value}_sf{sf}_t{threads_number}_*.txt')
    output_file_path = os.path.join(base_path, 
                                    f'filtered_sda_sdb_f{frequency_value}_sf{sf}_t{threads_number}.txt')

    # Check if there are any files that match the pattern
    files = glob.glob(file_path_pattern)
    if not files:
        print(f"No files found with pattern {file_path_pattern}. Skipping...")
        return  # Skip processing if no files are found

    print(f"Processing files with pattern {file_path_pattern}")
    save_filtered_sda_sdb(file_path_pattern, output_file_path, skip_zeros=True)

def save_filtered_sda_sdb(file_path_pattern, output_file_path, skip_zeros=False):
    """
    Filter and save the sda and sdb lines from the matched files.
    """
    output_data = {}

    # List all files matching the pattern
    files = glob.glob(file_path_pattern)
    
    for file_path in files:
        with open(file_path, 'r') as file:
            filename = os.path.basename(file_path)
            # Match the filename to extract parameters
            match = re.search(r'iostat_f(\d+\.\d+)_sf(\d+)_t(\d+)_q(\d+)\.txt', filename)
            if match:
                frequency, sf_number, t_number, query_number = match.groups()
                custom_header = f"Running TPC-H Query {query_number} (frequency {frequency}, sf {sf_number}, t {t_number})\n"
            else:
                print(f"Warning: Filename '{filename}' does not match expected pattern.")
                continue  # Skip files that don't match the expected pattern

            buffer = custom_header
            skip_next_sda = True
            skip_next_sdb = True
            device_header_written = False

            for line in file:
                if line.startswith("Running"):
                    skip_next_sda = True
                    skip_next_sdb = True
                    device_header_written = False
                    continue

                if "sda" in line:
                    if skip_next_sda:
                        skip_next_sda = False
                        continue
                    if not device_header_written:
                        device_header = ("Device             tps    kB_read/s    kB_wrtn/s    "
                                         "kB_dscd/s    kB_read    kB_wrtn    kB_dscd\n")
                        buffer += device_header
                        device_header_written = True

                if "sdb" in line:
                    if skip_next_sdb:
                        skip_next_sdb = False
                        continue

                if skip_zeros:
                    values = line.split()[2:]
                    try:
                        if all(float(val) == 0 for val in values if is_float(val)):
                            continue
                    except ValueError:
                        pass

                if "sda" in line or "sdb" in line:
                    buffer += line

            key = (float(frequency), int(sf_number), int(t_number), int(query_number))
            output_data[key] = buffer

    # Write the collected data to the output file, sorted by key
    with open(output_file_path, 'w') as outfile:
        for key in sorted(output_data.keys()):
            outfile.write(output_data[key])
            outfile.write("\n")

def is_float(value):
    """
    Check if the given value can be converted to a float.
    """
    try:
        float(value)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    
    # Determine the base_path from command-line arguments
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
    
    # Process each SF folder
    for sf_folder in sf_folders:
        sf_number_match = re.match(r'^SF(\d+)$', sf_folder)
        if sf_number_match:
            sf_number = sf_number_match.group(1)
            sf_folder_path = os.path.join(base_path, sf_folder)
            print(f"\nProcessing SF folder: {sf_folder} (sf={sf_number})")  # Debug
            find_frequency_and_threads(sf_folder_path, sf_number)
        else:
            print(f"Skipping unrecognized folder: {sf_folder}")  # Debug
