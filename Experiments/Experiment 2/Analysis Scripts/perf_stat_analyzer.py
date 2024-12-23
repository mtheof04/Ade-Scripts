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

def extract_system_metrics(file_path):
    """
    Extracts system metrics (Retiring, Bad Speculation, Frontend Bound, Backend Bound)
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
                # Identify the header line containing all required metrics
                if all(header.lower() in clean_line for header in metrics.keys()):
                    headers_line = clean_escape_codes(lines[i]).strip()
                    values_line = clean_escape_codes(lines[i + 1]).strip()
                    
                    # Split headers and values by two or more spaces to handle formatting
                    headers = re.split(r'\s{2,}', headers_line)
                    values = re.split(r'\s{2,}', values_line)
                    
                    # Create a mapping from header to value
                    for header, value in zip(headers, values):
                        header_title = header.strip()
                        for metric in metrics.keys():
                            if header_title.lower() == metric.lower():
                                # Extract the numerical percentage value
                                match = re.search(r'(\d+\.\d+)%', value)
                                if match:
                                    metrics[metric] = float(match.group(1))
                    break  # Stop after finding and processing the relevant metrics
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except IndexError:
        print(f"Warning: Expected values line after headers in {file_path}, but not found.")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return metrics

def extract_process_metrics(file_path):
    """
    Extracts process metrics from the perf_stats_process.txt file, including:
    - Insn per Cycle
    - Instructions
    - Branch Misses
    - LLC Load Misses
    """
    metrics = {
        'Insn per Cycle': 'N/A',
        'Instructions': 'N/A',
        'Branch Misses': 'N/A',
        'LLC Load Misses': 'N/A'
    }
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                clean_line = clean_escape_codes(line).strip().lower()
                
                # Extract 'Insn per Cycle'
                if 'insn per cycle' in clean_line:
                    match = re.search(r'([\d,]+\.\d+)\s+insn per cycle', clean_line)
                    if match:
                        insn_per_cycle = float(match.group(1).replace(',', ''))
                        metrics['Insn per Cycle'] = insn_per_cycle
                
                # Extract 'Instructions'
                if 'instructions' in clean_line:
                    match = re.search(r'([\d,]+)\s+instructions', clean_line)
                    if match:
                        instructions = int(match.group(1).replace(',', ''))
                        metrics['Instructions'] = instructions
                
                # Extract 'Branch Misses'
                if 'branch-misses' in clean_line:
                    match = re.search(r'([\d,]+)\s+branch-misses', clean_line)
                    if match:
                        branch_misses = int(match.group(1).replace(',', ''))
                        metrics['Branch Misses'] = branch_misses
                
                # Extract 'LLC-load-misses'
                if 'llc-load-misses' in clean_line:
                    match = re.search(r'([\d,]+)\s+llc-load-misses', clean_line)
                    if match:
                        llc_load_misses = int(match.group(1).replace(',', ''))
                        metrics['LLC Load Misses'] = llc_load_misses
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return metrics

def process_subdirectories(perf_stat_dir):
    """
    Processes each subdirectory within a PerfStat directory to extract performance metrics.
    """
    results = {}
    try:
        subdirs = [d for d in os.listdir(perf_stat_dir) if os.path.isdir(os.path.join(perf_stat_dir, d))]
    except Exception as e:
        print(f"Error accessing subdirectories in {perf_stat_dir}: {e}")
        return results
    
    for subdir in subdirs:
        subdir_path = os.path.join(perf_stat_dir, subdir)
        
        # Extract Process Metrics
        process_file = os.path.join(subdir_path, 'perf_stats_process.txt')
        process_metrics = extract_process_metrics(process_file)
        
        # Extract System Metrics
        system_file = os.path.join(subdir_path, 'perf_stats_system.txt')
        system_metrics = extract_system_metrics(system_file)
        
        # Compute Derived Metrics
        instructions = process_metrics.get('Instructions', 'N/A')
        branch_misses = process_metrics.get('Branch Misses', 'N/A')
        llc_load_misses = process_metrics.get('LLC Load Misses', 'N/A')
        
        if instructions != 'N/A' and instructions != 0:
            branch_misses_per_1000 = (branch_misses / instructions * 1000) if branch_misses != 'N/A' else 'N/A'
            llc_load_misses_per_1000 = (llc_load_misses / instructions * 1000) if llc_load_misses != 'N/A' else 'N/A'
        else:
            branch_misses_per_1000 = 'N/A'
            llc_load_misses_per_1000 = 'N/A'
        
        # Combine results with standardized metric names
        results[subdir] = {
            'Insn per Cycle': process_metrics.get('Insn per Cycle', 'N/A'),
            'Retiring': system_metrics.get('Retiring', 'N/A'),
            'Bad Speculation': system_metrics.get('Bad Speculation', 'N/A'),
            'Frontend Bound': system_metrics.get('Frontend Bound', 'N/A'),
            'Backend Bound': system_metrics.get('Backend Bound', 'N/A'),
            'Instructions': instructions,
            'Branch Misses': branch_misses,
            'LLC Load Misses': llc_load_misses,
            'Branch Misses per 1000 Instructions': branch_misses_per_1000,
            'LLC Load Misses per 1000 Instructions': llc_load_misses_per_1000
        }
    return results

def save_and_print_results(all_results, base_path):
    """
    Save the results to an Excel file named 'perf_stats_results.xlsx' inside each SF* folder
    and print the results to the console in the specified order.
    """
    sf_results = {}
    
    # Group results by SF folder
    for perf_stat_dir, subdir_results in all_results.items():
        sf_f_match = re.search(r'(SF\d+)/F([\d\.]+)', perf_stat_dir)
        if not sf_f_match:
            print(f"Warning: Could not determine SF and F structure for: {perf_stat_dir}")
            continue
        
        sf_folder = sf_f_match.group(1)
        f_folder = f"F{sf_f_match.group(2)}"
        if sf_folder not in sf_results:
            sf_results[sf_folder] = {}
        sf_results[sf_folder][f_folder] = subdir_results
    
    # Define the desired column order
    desired_order = [
        'Retiring',
        'Bad Speculation',
        'Frontend Bound',
        'Backend Bound',
        'Insn per Cycle',
        'Instructions',
        'Branch Misses',
        'LLC Load Misses',
        'Branch Misses per 1000 Instructions',
        'LLC Load Misses per 1000 Instructions'
    ]
    
    # Save results for each SF folder and print them
    for sf_folder, f_results in sf_results.items():
        sf_folder_path = os.path.join(base_path, sf_folder)
        if not os.path.exists(sf_folder_path):
            os.makedirs(sf_folder_path)
        
        sf_file = os.path.join(sf_folder_path, "perf_stats_results.xlsx")
        with pd.ExcelWriter(sf_file, engine='xlsxwriter') as writer:
            print(f"\nResults for {sf_folder}:")
            for f_folder, subdir_results in f_results.items():
                sheet_name = f_folder
                df = pd.DataFrame.from_dict(subdir_results, orient='index').reset_index()
                df = df.rename(columns={'index': 'Phase'})
                
                # Reorder columns
                df = df[['Phase'] + desired_order]
                
                # Optional: Format large numbers with commas for readability
                number_format_cols = ['Insn per Cycle', 'Instructions', 'Branch Misses', 'LLC Load Misses']
                for col in number_format_cols:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Print the results for the current folder
                print(f"\n  {f_folder}:")
                for _, row in df.iterrows():
                    phase = row['Phase']
                    print(f"    {phase}:")
                    for col in desired_order:
                        value = row[col]
                        # Determine suffix based on metric type
                        if 'per 1000 Instructions' in col:
                            suffix = ''
                        elif col == 'Insn per Cycle':
                            suffix = ''
                        elif any(keyword in col for keyword in ['Retiring', 'Speculation', 'Bound']):
                            suffix = ' %'
                        else:
                            suffix = ''
                        # Handle 'N/A' cases
                        if value != 'N/A':
                            print(f"      {col}: {value} {suffix}".strip())
                        else:
                            print(f"      {col}: {value}".strip())
        
        print(f"\nCombined results saved to {sf_file}")

def main(base_path):
    """
    Main function to traverse directories and extract metrics.
    """
    search_pattern = os.path.join(base_path, 'SF*', 'F*', 'PerfStat')
    perf_stat_dirs = glob.glob(search_pattern)
    
    if not perf_stat_dirs:
        print(f"No PerfStat directories found matching pattern: {search_pattern}")
        return {}
    
    all_results = {}
    for perf_stat_dir in perf_stat_dirs:
        print(f"Processing PerfStat directory: {perf_stat_dir}")
        results = process_subdirectories(perf_stat_dir)
        all_results[perf_stat_dir] = results
    return all_results

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    
    if not os.path.exists(base_path):
        print(f"Error: The base path '{base_path}' does not exist.")
        sys.exit(1)
    
    all_results = main(base_path)
    if all_results:
        save_and_print_results(all_results, base_path)
