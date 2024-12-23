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

def extract_insn_per_cycle(file_path):
    """
    Extracts the 'insn per cycle' value from the perf_stats_metrics.txt file.
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

def extract_perf_stats_metrics(file_path):
    """
    Extracts additional performance metrics from perf_stats_metrics.txt:
    - instructions
    - branch-misses
    - LLC-load-misses
    """
    metrics = {
        'instructions': None,
        'branch_misses': None,
        'llc_load_misses': None  # Changed key to lowercase
    }
    patterns = {
        'instructions': re.compile(r'([\d,]+)\s+instructions'),
        'branch_misses': re.compile(r'([\d,]+)\s+branch-misses'),
        'llc_load_misses': re.compile(r'([\d,]+)\s+llc-load-misses')  # Changed to lowercase
    }
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                clean_line = clean_escape_codes(line).replace(',', '').strip().lower()
                for key, pattern in patterns.items():
                    if metrics[key] is None:
                        match = pattern.search(clean_line)
                        if match:
                            metrics[key] = int(match.group(1))
                            print(f"Extracted {key}: {metrics[key]}")
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return metrics

def extract_system_metrics(file_path):
    """
    Extracts system metrics (retiring, bad speculation, frontend bound, backend bound)
    from the perf_stats_system.txt file.
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
                if all(header.lower() in clean_line for header in metrics.keys()):
                    headers = ["Retiring", "Bad Speculation", "Frontend Bound", "Backend Bound"]
                    positions = [clean_line.index(header.lower()) for header in headers]
                    if i + 1 < len(lines):
                        values_line = clean_escape_codes(lines[i + 1]).strip()
                        for j, pos in enumerate(positions):
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
    Processes each Iteration directory to extract metrics from perf_stats files.
    """
    metrics_file = os.path.join(perf_stat_dir, 'perf_stats_metrics.txt')
    system_file = os.path.join(perf_stat_dir, 'perf_stats_system.txt')

    insn_per_cycle = extract_insn_per_cycle(metrics_file)
    additional_metrics = extract_perf_stats_metrics(metrics_file)
    system_metrics = extract_system_metrics(system_file)

    branch_misses_per_1000_instructions = (
        (additional_metrics['branch_misses'] / additional_metrics['instructions'] * 1000)
        if additional_metrics['branch_misses'] is not None and additional_metrics['instructions'] else 'N/A'
    )
    LLC_load_misses_per_1000_instructions = (
        (additional_metrics['llc_load_misses'] / additional_metrics['instructions'] * 1000)
        if additional_metrics['llc_load_misses'] is not None and additional_metrics['instructions'] else 'N/A'
    )

    results = {
        'Retiring': system_metrics.get('Retiring', 'N/A'),
        'Bad Speculation': system_metrics.get('Bad Speculation', 'N/A'),
        'Frontend Bound': system_metrics.get('Frontend Bound', 'N/A'),
        'Backend Bound': system_metrics.get('Backend Bound', 'N/A'),
        'Insn per Cycle': insn_per_cycle if insn_per_cycle is not None else 'N/A',
        'Instructions': additional_metrics.get('instructions', 'N/A'),
        'Branch Misses': additional_metrics.get('branch_misses', 'N/A'),
        'LLC Load Misses': additional_metrics.get('llc_load_misses') if additional_metrics.get('llc_load_misses') is not None else 'N/A',
        'Branch Misses per 1000 Instructions': branch_misses_per_1000_instructions,
        'LLC Load Misses per 1000 Instructions': LLC_load_misses_per_1000_instructions
    }
    return results

def save_to_excel_with_averages(all_results, base_path):
    """
    Save the average results for Csv and Parquet into separate rows in a single sheet for each SF folder.
    """
    grouped_results = {}
    for iteration_dir, metrics in all_results.items():
        path_parts = iteration_dir.split(os.sep)
        sf_folder = next((part for part in path_parts if part.startswith('SF')), None)
        f_folder = next((part for part in path_parts if re.match(r'^F\d+(\.\d+)?$', part)), None)
        data_format = next((part for part in path_parts if part in ['Csv', 'Parquet']), None)

        if not (sf_folder and f_folder and data_format):
            print(f"Warning: Could not determine SF, F, or format for: {iteration_dir}")
            continue

        key = f"{f_folder} {data_format}"
        grouped_results.setdefault(sf_folder, {}).setdefault(key, []).append(metrics)

    for sf_folder, types in grouped_results.items():
        data = []
        print(f"\nResults for {sf_folder}:\n")
        for key, metrics_list in types.items():
            print(f"  Type: {key}")
            df = pd.DataFrame(metrics_list)
            for _, row in df.iterrows():
                print(row.to_dict())
            print("\n  Averages:")

            # Replace both 'N/A' and None with pd.NA
            df_cleaned = df.replace(['N/A', None], pd.NA)

            # Convert columns to numeric where possible
            numeric_df = df_cleaned.apply(pd.to_numeric, errors='coerce')

            # Calculate the average for each type
            avg_row = numeric_df.mean(skipna=True).to_dict()

            # Replace NaN with 'N/A'
            avg_row = {k: (v if pd.notna(v) else 'N/A') for k, v in avg_row.items()}

            # Add the 'Type' key
            avg_row['Type'] = key
            data.append(avg_row)

            # Print the average row
            print(avg_row)

        # Create a DataFrame for this SF folder
        result_df = pd.DataFrame(data)

        # Ensure 'Type' is the first column
        columns = ['Type'] + [col for col in result_df.columns if col != 'Type']
        result_df = result_df[columns]

        # Define the Excel file path
        excel_file = os.path.join(base_path, sf_folder, f"perf_stats_{sf_folder}_combined.xlsx")

        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            print(f"\nSaving averages to {excel_file}")
            result_df.to_excel(writer, sheet_name='Metrics', index=False)
            print(f"Averages saved to {excel_file}")

def main(base_path):
    """
    Main function to traverse directories and extract metrics.
    """
    search_pattern = os.path.join(base_path, 'SF*', 'F*', '*', 'Iteration*')
    iteration_dirs = glob.glob(search_pattern)

    if not iteration_dirs:
        print(f"No Iteration directories found matching pattern: {search_pattern}")
        return {}

    all_results = {}
    for iteration_dir in iteration_dirs:
        print(f"Processing Iteration directory: {iteration_dir}")
        results = process_files(iteration_dir)
        all_results[iteration_dir] = results
    return all_results

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)

    all_results = main(base_path)
    if all_results:
        save_to_excel_with_averages(all_results, base_path)
