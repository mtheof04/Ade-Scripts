import os
import sys
import glob
import re

def strip_unwanted_sections(lines, iteration_number, keep_header=False):
    """
    Function to strip unwanted sections from iostat files and skip everything until the second 'avg-cpu:' line.
    It also adds the iteration number to the output.
    """
    output_lines = [f"Iteration {iteration_number}\n"]  # Add the iteration number at the beginning
    avg_cpu_count = 0
    inside_skip_section = True

    for line in lines:
        # Check for 'avg-cpu:' and count its occurrence
        if 'avg-cpu:' in line:
            avg_cpu_count += 1
        
        # If we've reached the second 'avg-cpu:', stop skipping lines
        if avg_cpu_count == 2:
            inside_skip_section = False

        # Add lines after the second 'avg-cpu:' occurrence
        if not inside_skip_section:
            output_lines.append(line)

    return output_lines

def process_iostat_files(base_path, sf_folder, frequency, threads):
    """
    Process iostat files by concatenating them while keeping the header and skipping sections 
    until the second 'avg-cpu:' line is found in each file.
    Also adds the iteration number at the beginning of each iteration's data.
    """
    # Convert frequency to lowercase format (e.g., F2.0 to f2.0)
    formatted_frequency = frequency.lower()
    formatted_threads = threads.lower()

    iostat_path = os.path.join(base_path, sf_folder, frequency, threads, 'Iostat', 'Iterations')  # Updated iteration folder path
    aggregated_path = os.path.join(base_path, sf_folder, frequency, threads, 'Iostat', 'Aggregated')  # Aggregated folder path

    # Create Aggregated folder if it does not exist
    os.makedirs(aggregated_path, exist_ok=True)

    file_pattern = f'iostat_{formatted_frequency}_{sf_folder.lower()}_{formatted_threads}_q*_it*.txt'  # Updated pattern to match the new format

    # Get all the files matching the pattern (sorted by query number and iteration)
    file_paths = sorted(glob.glob(os.path.join(iostat_path, file_pattern)))

    # Group the files by query number (q1, q2, etc.)
    queries = {}
    for file_path in file_paths:
        query_number = file_path.split('_q')[1].split('_')[0]  # Extract query number from filename
        iteration_number = int(file_path.split('_it')[1].split('.')[0])  # Extract iteration number from filename and convert to integer
        if query_number not in queries:
            queries[query_number] = []
        queries[query_number].append((file_path, iteration_number))

    # Sort the iterations for each query
    for query in queries:
        queries[query] = sorted(queries[query], key=lambda x: x[1])

    # Process each query group
    for query, files in queries.items():
        concatenated_output = ""
        
        # Process the first file (include the header, skip until the second 'avg-cpu:' line)
        first_file_path, first_iteration_number = files[0]
        with open(first_file_path, 'r') as file:
            lines = file.readlines()
            concatenated_output += ''.join(strip_unwanted_sections(lines, first_iteration_number, keep_header=True))

        # Process the subsequent files (skip until the second 'avg-cpu:' line, no header)
        for path, iteration_number in files[1:]:
            with open(path, 'r') as file:
                lines = file.readlines()
                concatenated_output += ''.join(strip_unwanted_sections(lines, iteration_number, keep_header=False))

        # Save the concatenated output in the Aggregated folder for the current query
        output_file_path = os.path.join(aggregated_path, f'iostat_{formatted_frequency}_{sf_folder.lower()}_{formatted_threads}_q{query}.txt')
        with open(output_file_path, 'w') as output_file:
            output_file.write(concatenated_output)
        print(f"Output file will be: {output_file_path}")  # Log the saved output file

def find_frequency_and_threads(base_path):
    """
    Find all SF folders dynamically (e.g., SF100, SF300) and process each frequency-thread combination within them.
    """
    sf_folders = [sf for sf in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, sf)) and sf.startswith("SF")]
    print(f"Found SF folders: {sf_folders}")

    for sf_folder in sf_folders:
        sf_folder_path = os.path.join(base_path, sf_folder)

        # Find all frequency folders (e.g., F2.0, F2.5) in the SF folder
        frequency_folders = [f for f in os.listdir(sf_folder_path) if re.match(r'^F\d+\.\d+$', f) and os.path.isdir(os.path.join(sf_folder_path, f))]
        print(f"Found frequency folders in {sf_folder}: {frequency_folders}")

        for frequency in frequency_folders:
            # Within each frequency folder, find thread folders (e.g., T20, T40)
            threads_path = os.path.join(sf_folder_path, frequency)
            thread_folders = [t for t in os.listdir(threads_path) if re.match(r'^T\d+$', t) and os.path.isdir(os.path.join(threads_path, t))]
            print(f"Found thread folders in {frequency}: {thread_folders}")

            for threads in thread_folders:
                process_iostat_files(base_path, sf_folder, frequency, threads)

# Main execution starts here
if len(sys.argv) > 1:
    base_path = sys.argv[1]
else:
    print("Usage: python3 script.py <base_path>")
    sys.exit(1)

find_frequency_and_threads(base_path)
