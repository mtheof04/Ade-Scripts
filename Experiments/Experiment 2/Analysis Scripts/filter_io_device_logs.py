import os
import sys
import re

def save_filtered_sda_sdb(file_path, query_type, iteration, output_file_path, skip_zeros=False, write_iteration=False):
    """Filter and save 'sda' and 'sdb' data for the query phase."""
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found for {query_type} in {iteration}. Skipping.")
        return

    # Initialize buffer for the current phase
    buffer = ""

    # Flags to skip the first occurrence of 'sda' and 'sdb'
    skip_next_sda = True
    skip_next_sdb = True
    device_header_written = False

    with open(file_path, 'r') as file:
        for line in file:
            # Check for 'sda' and 'sdb' and skip the first occurrence
            if "sda" in line:
                if skip_next_sda:
                    skip_next_sda = False
                    continue
                if not device_header_written:
                    buffer += get_device_header()
                    device_header_written = True

            if "sdb" in line:
                if skip_next_sdb:
                    skip_next_sdb = False
                    continue

            # Optionally skip lines where all values are zero after 'sda' or 'sdb'
            if skip_zeros:
                values = line.split()[2:]  # Skip device name and first value
                if all_zero(values):
                    continue

            # Add the line to the buffer
            if "sda" in line or "sdb" in line:
                buffer += line

    # Write the buffer to the output file with the query type as a header
    with open(output_file_path, 'a') as outfile:
        if write_iteration:  # Write iteration only once per iteration
            outfile.write(f"{iteration}\n\n")
        outfile.write(buffer)
        outfile.write("\n")

def get_device_header():
    """Returns the device header for the iostat output."""
    return "Device             tps    kB_read/s    kB_wrtn/s    kB_dscd/s    kB_read    kB_wrtn    kB_dscd\n"

def all_zero(values):
    """Checks if all values in the list are zero."""
    return all(float(val) == 0 for val in values if is_float(val))

def is_float(value):
    """Helper function to determine if a value can be converted to a float.""" 
    try:
        float(value)
        return True
    except ValueError:
        return False

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    sf_folders = [f for f in os.listdir(base_path) if f.startswith("SF") and os.path.isdir(os.path.join(base_path, f))]

    iteration_folder_pattern = re.compile(r'^Iteration(\d+)$')
    query_order = ['Sequential', 'Joins', 'Aggregations', 'Sorting', 'Filtering']

    for sf_folder in sf_folders:
        sf_path = os.path.join(base_path, sf_folder)
        additional_folders = [f for f in os.listdir(sf_path) if f.startswith("F") and os.path.isdir(os.path.join(sf_path, f))]

        for additional_folder in additional_folders:
            iterations_folder_path = os.path.join(sf_path, additional_folder, 'Iterations')

            if not os.path.exists(iterations_folder_path):
                print(f"Warning: Folder {iterations_folder_path} does not exist. Skipping.")
                continue

            output_file_path_skip = os.path.join(sf_path, additional_folder, 'filtered_sda_sdb_skip_first_iteration_logs.txt')
            output_file_path_include = os.path.join(sf_path, additional_folder, 'filtered_sda_sdb_include_first_iteration_logs.txt')
            
            if os.path.exists(output_file_path_skip):
                os.remove(output_file_path_skip)
            if os.path.exists(output_file_path_include):
                os.remove(output_file_path_include)

            iteration_folders = [f for f in os.listdir(iterations_folder_path) if iteration_folder_pattern.match(f)]
            iteration_folders = sorted(iteration_folders, key=lambda x: int(iteration_folder_pattern.match(x).group(1)))

            for query_type in query_order:
                with open(output_file_path_skip, 'a') as outfile:
                    outfile.write(f"{query_type}\n\n")
                with open(output_file_path_include, 'a') as outfile:
                    outfile.write(f"{query_type}\n\n")

                for subfolder in iteration_folders:
                    iteration_folder = os.path.join(iterations_folder_path, subfolder)
                    match = iteration_folder_pattern.match(subfolder)
                    if not os.path.isdir(iteration_folder) or not match:
                        continue

                    iteration_number = int(match.group(1))
                    iteration_name = f"Iteration {iteration_number}"

                    subfolders = os.listdir(iteration_folder)
                    for query_subfolder in subfolders:
                        query_type_folder = os.path.join(iteration_folder, query_subfolder)
                        folder_name = os.path.basename(query_subfolder).capitalize()

                        if folder_name != query_type:
                            continue

                        file_path = os.path.join(query_type_folder, 'iostat.log')

                        if iteration_number != 1:
                            save_filtered_sda_sdb(file_path, query_type, iteration_name, output_file_path_skip, skip_zeros=True, write_iteration=True)

                        save_filtered_sda_sdb(file_path, query_type, iteration_name, output_file_path_include, skip_zeros=True, write_iteration=True)