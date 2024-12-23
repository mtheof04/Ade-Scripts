import os
import sys
import re

def save_filtered_sda_sdb(file_path, iteration, output_file_path, skip_zeros=False, write_iteration=False):
    """Filter and save 'sda' and 'sdb' data for the query phase."""
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found in {iteration}. Skipping.")
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

    # Write the buffer to the output file
    with open(output_file_path, 'a') as outfile:
        if write_iteration:
            outfile.write(f"{iteration}\n\n")
        outfile.write(buffer)
        outfile.write("\n")  # Add a newline after each iteration entry

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

    # Compile regex to match SF folders (e.g., SF100, SF300)
    sf_folder_pattern = re.compile(r'^SF\d+$')

    # Compile regex to match F folders (e.g., F1.0, F1.5, F2.0, F2.5)
    f_folder_pattern = re.compile(r'^F\d+(\.\d+)?$')

    # Compile regex to match Iteration folders (e.g., Iteration1, Iteration2)
    iteration_folder_pattern = re.compile(r'^Iteration(\d+)$')

    # Iterate through all items in base_path to find SF folders
    try:
        sf_folders = [
            f for f in os.listdir(base_path)
            if sf_folder_pattern.match(f) and os.path.isdir(os.path.join(base_path, f))
        ]
    except FileNotFoundError:
        print(f"Error: Base path '{base_path}' does not exist.")
        sys.exit(1)

    if not sf_folders:
        print(f"No SF folders found in base path '{base_path}'.")
        sys.exit(1)

    print(f"Found SF folders: {sf_folders}")

    # Process each SF folder
    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)
        print(f"\nProcessing SF folder: {sf_folder_path}")

        # Dynamically find F folders within the SF folder
        try:
            f_folders = [
                f for f in os.listdir(sf_folder_path)
                if f_folder_pattern.match(f) and os.path.isdir(os.path.join(sf_folder_path, f))
            ]
        except FileNotFoundError:
            print(f"Warning: Could not list F folders in {sf_folder_path}. Skipping.")
            continue

        if not f_folders:
            print(f"No F folders found in {sf_folder_path}. Skipping.")
            continue

        print(f"Found F folders: {f_folders}")

        # Process each F folder
        for f_folder in f_folders:
            f_folder_path = os.path.join(sf_folder_path, f_folder)
            print(f"\nProcessing F folder: {f_folder_path}")

            # Loop through 'Csv' and 'Parquet' subfolders
            for data_type in ['Csv', 'Parquet']:
                data_type_folder = os.path.join(f_folder_path, data_type)
                if not os.path.exists(data_type_folder):
                    print(f"Warning: {data_type_folder} does not exist in {f_folder}. Skipping.")
                    continue

                print(f"Processing data type folder: {data_type_folder}")

                # Output file path for the filtered log in the respective data type folder
                output_file_path = os.path.join(data_type_folder, 'filtered_sda_sdb_logs.txt')

                # Clear the output file if it already exists
                if os.path.exists(output_file_path):
                    os.remove(output_file_path)
                    print(f"Existing output file '{output_file_path}' removed.")

                # Find all Iteration folders inside the data type folder
                try:
                    iteration_folders = [
                        f for f in os.listdir(data_type_folder)
                        if iteration_folder_pattern.match(f) and os.path.isdir(os.path.join(data_type_folder, f))
                    ]
                except FileNotFoundError:
                    print(f"Warning: Could not list directories in {data_type_folder}. Skipping.")
                    continue

                if not iteration_folders:
                    print(f"No Iteration folders found in {data_type_folder}. Skipping.")
                    continue

                # Sort iteration folders numerically by the number in Iteration
                iteration_folders = sorted(
                    iteration_folders,
                    key=lambda x: int(iteration_folder_pattern.match(x).group(1))
                )

                print(f"Found Iteration folders: {iteration_folders}")

                for subfolder in iteration_folders:
                    iteration_folder = os.path.join(data_type_folder, subfolder)
                    match = iteration_folder_pattern.match(subfolder)
                    if not match:
                        print(f"Skipping non-matching folder: {subfolder}")
                        continue

                    iteration_number = int(match.group(1))  # Extract the iteration number
                    iteration_name = f"Iteration {iteration_number}"

                    file_path = os.path.join(iteration_folder, 'iostat.log')

                    print(f"Saving filtered data for {iteration_name} from '{file_path}'")
                    save_filtered_sda_sdb(
                        file_path,
                        iteration_name,
                        output_file_path,
                        skip_zeros=True,
                        write_iteration=True
                    )

    print("\nProcessing completed.")
