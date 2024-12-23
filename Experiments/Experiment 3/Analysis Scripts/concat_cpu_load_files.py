import os
import sys
import glob
import re

def sort_by_iteration(file_name):
    """Extract iteration number from the filename to sort by it."""
    match = re.search(r'_it(\d+)$', file_name)
    if match:
        return int(match.group(1))
    return 0

def skip_linux_line_and_blanks(lines):
    """Skip the 'Linux...' line and any subsequent blank lines."""
    data_lines = []
    skip = True
    for line in lines:
        if skip:
            if line.startswith("Linux"):
                continue  # Skip the 'Linux...' line
            elif line.strip() == '':
                continue  # Skip blank lines
            else:
                skip = False
                data_lines.append(line)
        else:
            data_lines.append(line)
    return data_lines

def ensure_newlines(lines):
    """Ensure each line ends with a newline character."""
    return [line if line.endswith('\n') else line + '\n' for line in lines]

def concatenate_cpu_load_files(base_path, sf_folder, frequency, threads, query):
    # Adjusted path for input and output
    cpu_load_path = os.path.join(base_path, sf_folder, frequency, threads, 'CpuLoad', 'Iterations')
    aggregated_path = os.path.join(base_path, sf_folder, frequency, threads, 'CpuLoad', 'Aggregated')

    # Ensure the output directory exists
    if not os.path.exists(aggregated_path):
        os.makedirs(aggregated_path)

    # Convert frequency and threads to lowercase to match the filename pattern
    file_pattern = os.path.join(cpu_load_path, f"cpu_load_{frequency.lower()}_{sf_folder.lower()}_{threads.lower()}_q{query}_it*.txt")
    run_files = glob.glob(file_pattern)

    if not run_files:
        print(f"No files found matching: {file_pattern}")
        return

    # Define output file path in the 'Aggregated' folder
    output_file = os.path.join(aggregated_path, f"cpu_load_{frequency.lower()}_{sf_folder.lower()}_{threads.lower()}_q{query}.txt")
    print(f"Output file will be: {output_file}")

    header_written = False
    with open(output_file, 'w') as outfile:
        for file in run_files:
            with open(file, 'r') as infile:
                lines = infile.readlines()

                if lines:
                    if not header_written:
                        lines = ensure_newlines(lines)
                        outfile.writelines(lines)
                        header_written = True
                    else:
                        data_lines = skip_linux_line_and_blanks(lines)
                        data_lines = ensure_newlines(data_lines)
                        outfile.writelines(data_lines)
                    
                    outfile.write('\n')

def process_all_queries(base_path, sf_folder, frequency, threads):
    cpu_load_path = os.path.join(base_path, sf_folder, frequency, threads, 'CpuLoad', 'Iterations')

    # Convert frequency and threads to lowercase for filename matching
    query_files = glob.glob(os.path.join(cpu_load_path, f"cpu_load_{frequency.lower()}_{sf_folder.lower()}_{threads.lower()}_q*_it*.txt"))

    # Extract unique query numbers from filenames
    query_numbers = sorted(set(re.search(r'_q(\d+)_', f).group(1) for f in query_files if re.search(r'_q(\d+)_', f)))

    if not query_numbers:
        print(f"No query files found in {cpu_load_path}")
        return

    # Process each query
    for query in query_numbers:
        concatenate_cpu_load_files(base_path, sf_folder, frequency, threads, query)

def find_frequency_and_threads(base_path):
    # Find all SF folders dynamically (e.g., SF100, SF300)
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
                process_all_queries(base_path, sf_folder, frequency, threads)

# Main execution starts here
if len(sys.argv) > 1:
    base_path = sys.argv[1]
else:
    print("Usage: python3 script.py <base_path>")
    sys.exit(1)

find_frequency_and_threads(base_path)
