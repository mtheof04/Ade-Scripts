import os
import glob
import re
import sys
import pandas as pd

def clean_escape_codes(text):
    """
    Removes ANSI escape sequences from text.
    """
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def extract_process_metrics(file_path):
    """
    Extracts process metrics from the perf_stats_process_f*.txt file, including:
    - Instructions
    - Branch Misses
    - LLC Load Misses
    """
    metrics = {
        'Instructions': 'N/A',
        'Branch Misses': 'N/A',
        'LLC Load Misses': 'N/A'
    }
    # Updated patterns to be lowercase to match the lowercased lines
    patterns = {
        'Instructions': re.compile(r'([\d,]+)\s+instructions'),
        'Branch Misses': re.compile(r'([\d,]+)\s+branch-misses'),
        'LLC Load Misses': re.compile(r'([\d,]+)\s+llc-load-misses')
    }
    try:
        with open(file_path, 'r') as f:
            for line in f:
                clean_line = clean_escape_codes(line).strip().lower()
                for key, pattern in patterns.items():
                    match = pattern.search(clean_line)
                    if match and metrics[key] == 'N/A':
                        # Remove commas and convert to integer
                        metrics[key] = int(match.group(1).replace(',', ''))
        return metrics
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return metrics

def extract_insn_per_cycle(file_path):
    """
    Extracts the 'insn per cycle' value from the perf_stats_process_f*.txt file.
    """
    pattern = re.compile(r'(\d+\.\d+)\s+insn per cycle')
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = clean_escape_codes(line)
                match = pattern.search(line)
                if match:
                    return float(match.group(1))
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return None

def extract_system_metrics(file_path):
    """
    Extracts system metrics (retiring, bad speculation, frontend bound, backend bound)
    from the perf_stats_system_f*.txt file.
    """
    metrics = {
        'Retiring': 'N/A',
        'Bad Speculation': 'N/A',
        'Frontend Bound': 'N/A',
        'Backend Bound': 'N/A'
    }
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                clean_line = clean_escape_codes(line).strip().lower()
                # Identify the header line containing all required metrics
                if all(header.lower() in clean_line for header in metrics.keys()):
                    headers = ["Retiring", "Bad Speculation", "Frontend Bound", "Backend Bound"]
                    positions = [clean_line.index(header.lower()) for header in headers]

                    # Ensure there is a next line for values
                    if i + 1 < len(lines):
                        values_line = clean_escape_codes(lines[i + 1]).strip()
                        for j, pos in enumerate(positions):
                            # Extract the percentage value near the header's position
                            match = re.search(r'(\d+\.\d+)%', values_line[pos:])
                            if match:
                                metrics[headers[j]] = float(match.group(1))
                    break
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return metrics

def process_files(perf_stat_dir):
    """
    Processes each PerfStat directory to extract metrics from query-specific files.
    """
    results = {}
    # Patterns to match perf_stats_process and perf_stats_system files with queries
    process_pattern = os.path.join(perf_stat_dir, 'perf_stats_process_f*_sf*_t*_q*.txt')
    system_pattern = os.path.join(perf_stat_dir, 'perf_stats_system_f*_sf*_t*_q*.txt')

    process_files = glob.glob(process_pattern)
    system_files = glob.glob(system_pattern)

    # Create a mapping from query number to file paths
    query_files = {}

    for file_path in process_files:
        match = re.search(r'_q(\d+)\.txt$', file_path)
        if match:
            q_num = int(match.group(1))
            query_files.setdefault(q_num, {})['process'] = file_path

    for file_path in system_files:
        match = re.search(r'_q(\d+)\.txt$', file_path)
        if match:
            q_num = int(match.group(1))
            query_files.setdefault(q_num, {})['system'] = file_path

    # Iterate through queries 1 to 22
    for q in range(1, 23):
        files = query_files.get(q, {})
        if not files:
            print(f"Warning: No files found for Query {q} in {perf_stat_dir}")
            results[q] = {
                'Insn per Cycle': 'N/A',
                'Instructions': 'N/A',
                'Branch Misses': 'N/A',
                'LLC Load Misses': 'N/A',
                'Branch Misses / Instructions *1000': 'N/A',
                'LLC Load Misses / Instructions *1000': 'N/A',
                'Retiring': 'N/A',
                'Bad Speculation': 'N/A',
                'Frontend Bound': 'N/A',
                'Backend Bound': 'N/A'
            }
            continue

        # Extract Insn per Cycle
        insn_per_cycle = extract_insn_per_cycle(files.get('process', ''))

        # Extract Process Metrics
        process_metrics = extract_process_metrics(files.get('process', ''))

        # Extract System Metrics
        system_metrics = extract_system_metrics(files.get('system', ''))

        # Retrieve Metrics
        instructions = process_metrics.get('Instructions')
        branch_misses = process_metrics.get('Branch Misses')
        llc_load_misses = process_metrics.get('LLC Load Misses')

        # Initialize ratios as 'N/A'
        branch_misses_per_instr = 'N/A'
        llc_load_misses_per_instr = 'N/A'

        # Check if metrics are valid integers before performing division
        if (
            isinstance(instructions, int) and instructions != 0 and
            isinstance(branch_misses, int) and
            isinstance(llc_load_misses, int)
        ):
            branch_misses_per_instr = (branch_misses / instructions) * 1000
            llc_load_misses_per_instr = (llc_load_misses / instructions) * 1000
        else:
            # Optional: Log which metrics are invalid
            if instructions == 'N/A' or not isinstance(instructions, int):
                print(f"  Query {q}: Invalid 'Instructions' value: {instructions}")
            if branch_misses == 'N/A' or not isinstance(branch_misses, int):
                print(f"  Query {q}: Invalid 'Branch Misses' value: {branch_misses}")
            if llc_load_misses == 'N/A' or not isinstance(llc_load_misses, int):
                print(f"  Query {q}: Invalid 'LLC Load Misses' value: {llc_load_misses}")

        # Combine results
        results[q] = {
            'Insn per Cycle': insn_per_cycle if insn_per_cycle is not None else 'N/A',
            'Instructions': instructions if isinstance(instructions, int) else 'N/A',
            'Branch Misses': branch_misses if isinstance(branch_misses, int) else 'N/A',
            'LLC Load Misses': llc_load_misses if isinstance(llc_load_misses, int) else 'N/A',
            'Branch Misses / Instructions *1000': branch_misses_per_instr if isinstance(branch_misses_per_instr, float) else 'N/A',
            'LLC Load Misses / Instructions *1000': llc_load_misses_per_instr if isinstance(llc_load_misses_per_instr, float) else 'N/A',
            'Retiring': system_metrics.get('Retiring', 'N/A'),
            'Bad Speculation': system_metrics.get('Bad Speculation', 'N/A'),
            'Frontend Bound': system_metrics.get('Frontend Bound', 'N/A'),
            'Backend Bound': system_metrics.get('Backend Bound', 'N/A')
        }

    return results

def save_to_excel(all_results, base_path):
    """
    Save the aggregated results to separate Excel files within each SF folder.
    Each Excel file contains multiple sheets corresponding to different F* folders.
    The Excel files are named 'perf_stats_results.xlsx' and placed inside their respective SF folders.
    """
    # Group results by SF folder
    sf_grouped_results = {}
    for perf_stat_dir, queries in all_results.items():
        # Extract SF folder from the directory path
        path_parts = perf_stat_dir.split(os.sep)
        try:
            sf_folder = next(part for part in path_parts if part.startswith('SF'))
        except StopIteration:
            print(f"Warning: Could not determine SF structure for: {perf_stat_dir}")
            continue

        sf_grouped_results.setdefault(sf_folder, {}).update({perf_stat_dir: queries})

    for sf_folder, perf_stats in sf_grouped_results.items():
        # Define the path to the SF folder
        sf_path = os.path.join(base_path, sf_folder)

        # Define the Excel file path within the SF folder
        excel_file = os.path.join(sf_path, "perf_stats_results.xlsx")

        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            print(f"\nSaving results to {excel_file}")
            for perf_stat_dir, queries in perf_stats.items():
                # Extract F* from the directory path
                path_parts = perf_stat_dir.split(os.sep)
                try:
                    f_folder = next(part for part in path_parts if re.match(r'^F\d+(\.\d+)?$', part))
                except StopIteration:
                    print(f"Warning: Could not determine F structure for: {perf_stat_dir}")
                    continue

                # Create a unique sheet name using F* and T* identifiers
                sheet_name = f"{f_folder}"
                try:
                    t_folder = next(part for part in path_parts if part.startswith('T'))
                    sheet_name = f"{f_folder}_{t_folder}"
                except StopIteration:
                    pass  # If no T* folder is found, keep sheet_name as F*

                # Excel sheet names have a maximum length of 31 characters
                sheet_name = sheet_name[:31]

                # Prepare DataFrame
                df = pd.DataFrame.from_dict(queries, orient='index').reset_index()
                df = df.rename(columns={'index': 'Query'})
                df = df.sort_values('Query')  # Ensure queries are in order

                # Reorder columns as desired
                desired_order = [
                    'Query',
                    'Insn per Cycle',
                    'Instructions',
                    'Branch Misses',
                    'LLC Load Misses',
                    'Branch Misses / Instructions *1000',
                    'LLC Load Misses / Instructions *1000',
                    'Retiring',
                    'Bad Speculation',
                    'Frontend Bound',
                    'Backend Bound'
                ]
                df = df[desired_order]

                # Write DataFrame to the respective sheet
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                print(f"\n  Sheet '{sheet_name}':")
                for _, row in df.iterrows():
                    print(f"    Query {int(row['Query'])}:")
                    print(f"      Insn per Cycle: {row['Insn per Cycle']}")
                    print(f"      Instructions: {row['Instructions']}")
                    print(f"      Branch Misses: {row['Branch Misses']}")
                    print(f"      LLC Load Misses: {row['LLC Load Misses']}")
                    print(f"      Branch Misses / Instructions *1000: {row['Branch Misses / Instructions *1000']}")
                    print(f"      LLC Load Misses / Instructions *1000: {row['LLC Load Misses / Instructions *1000']}")
                    print(f"      Retiring: {row['Retiring']}%")
                    print(f"      Bad Speculation: {row['Bad Speculation']}%")
                    print(f"      Frontend Bound: {row['Frontend Bound']}%")
                    print(f"      Backend Bound: {row['Backend Bound']}%")

        print(f"\nAll results for {sf_folder} have been successfully saved to {excel_file}")

def main(base_path):
    """
    Main function to traverse directories and extract metrics.
    """
    # Updated search pattern to include T* directory
    search_pattern = os.path.join(base_path, 'SF*', 'F*', 'T*', 'PerfStat')
    perf_stat_dirs = glob.glob(search_pattern)

    if not perf_stat_dirs:
        print(f"No PerfStat directories found matching pattern: {search_pattern}")
        return {}

    all_results = {}
    for perf_stat_dir in perf_stat_dirs:
        print(f"Processing PerfStat directory: {perf_stat_dir}")
        results = process_files(perf_stat_dir)
        all_results[perf_stat_dir] = results
    return all_results

if __name__ == "__main__":
    # Get from command-line arguments
    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)

    all_results = main(base_path)
    if all_results:
        save_to_excel(all_results, base_path)
