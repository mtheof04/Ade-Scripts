import os
import re
import sys
import pandas as pd


def extract_execution_time(file_path):
    """Extract the Total Query Execution Time from the file."""
    try:
        with open(file_path, 'r') as file:
            for line in file:
                match = re.search(r'Total Query Execution Time(?: for Iteration \d+)?:\s*(\d+)(?:\s+seconds)?', line)
                if match:
                    execution_time = int(match.group(1))
                    print(f"Extracted execution time: {execution_time} seconds from {file_path}")
                    return execution_time
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return 0


def process_query_phase(iterations_path, query_phase):
    """Process all iterations for a given query phase and calculate metrics."""
    execution_times = []
    iteration_pattern = re.compile(r'^Iteration(\d+)$')

    # Detect all iteration folders dynamically
    try:
        iterations = [folder for folder in os.listdir(iterations_path) if iteration_pattern.match(folder)]
    except FileNotFoundError:
        print(f"Error: Iterations path does not exist: {iterations_path}")
        return 0, 0, 0

    iterations.sort(key=lambda x: int(iteration_pattern.match(x).group(1)))  # Sort Iteration folders by number

    # Process each iteration and collect valid data
    for iteration_folder in iterations:
        file_path = os.path.join(iterations_path, iteration_folder, query_phase.capitalize(), "ilo_power_timestamps.txt")
        if not os.path.exists(file_path):
            print(f"Skipping missing file: {file_path}")
            continue

        print(f"Processing file: {file_path}")
        execution_time = extract_execution_time(file_path)
        if execution_time > 0:
            print(f"Found execution time: {execution_time} seconds")
            execution_times.append(execution_time)

    print(f"Collected execution times for {query_phase}: {execution_times}")

    # Calculate metrics only if there are valid execution times
    if execution_times:
        avg_time = sum(execution_times) / len(execution_times)
        sample_std_dev = pd.Series(execution_times).std(ddof=1)  # Sample standard deviation
        std_error = sample_std_dev / (len(execution_times) ** 0.5)  # Standard error
    else:
        avg_time = sample_std_dev = std_error = 0
        print(f"No valid data found for {query_phase.capitalize()}.")

    print(f"Metrics for {query_phase} - Avg: {avg_time:.2f}, Std Dev: {sample_std_dev:.2f}, Std Error: {std_error:.2f}\n")

    return avg_time, sample_std_dev, std_error


def process_folders(folder_path):
    """Process all query phases within the Iterations folder of a base folder."""
    metrics = {}
    iterations_path = os.path.join(folder_path, "Iterations")

    if not os.path.exists(iterations_path):
        print(f"Warning: Folder {iterations_path} does not exist. Skipping.\n")
        return metrics

    first_iteration_path = os.path.join(iterations_path, "Iteration1")
    if not os.path.exists(first_iteration_path):
        print(f"Error: No Iteration1 folder found in {iterations_path}. Skipping.\n")
        return metrics

    query_phases = [d for d in os.listdir(first_iteration_path) if os.path.isdir(os.path.join(first_iteration_path, d))]

    for phase in query_phases:
        print(f"Processing query phase: {phase}")
        avg_time, sample_std_dev, std_error = process_query_phase(iterations_path, phase)
        metrics[phase.capitalize()] = (avg_time, sample_std_dev, std_error)
        print(f"{phase.capitalize()} - Avg: {avg_time:.2f} s, Sample Std Dev: {sample_std_dev:.2f} s, Std Error: {std_error:.2f} s\n")

    return metrics


def main(base_path):
    """Main function to dynamically process SF and F folders."""
    try:
        sf_folders = [sf for sf in os.listdir(base_path) if re.match(r'^SF\d+$', sf) and os.path.isdir(os.path.join(base_path, sf))]
    except FileNotFoundError:
        print(f"Error: Base path does not exist: {base_path}")
        return

    if not sf_folders:
        print(f"No SF folders found in {base_path}. Exiting.")
        return

    for sf_folder in sf_folders:
        sf_path = os.path.join(base_path, sf_folder)
        combined_data = {}

        # Find all F folders dynamically inside each SF folder
        try:
            f_folders = [f for f in os.listdir(sf_path) if re.match(r'^F\d+\.\d+$', f) and os.path.isdir(os.path.join(sf_path, f))]
        except FileNotFoundError:
            print(f"Warning: Could not list folders in {sf_path}. Skipping SF folder.\n")
            continue

        for f_folder in f_folders:
            folder_path = os.path.join(sf_path, f_folder)
            print(f"Processing folder: {folder_path}")
            metrics = process_folders(folder_path)

            if metrics:
                df = pd.DataFrame(
                    [(phase, avg, std_dev, std_err) for phase, (avg, std_dev, std_err) in metrics.items()],
                    columns=['Queries', 'Avg Time (s)', 'Sample Std Dev (s)', 'Std Error (s)']
                )
                combined_data[f_folder] = df
            else:
                print(f"No metrics collected for folder: {folder_path}\n")

        # Save combined results for the SF folder
        if combined_data:
            combined_excel_output_path = os.path.join(sf_path, f'query_execution_times_avg_combined.xlsx')
            try:
                with pd.ExcelWriter(combined_excel_output_path, engine='xlsxwriter') as writer:
                    for sheet_name, data in combined_data.items():
                        data.to_excel(writer, sheet_name=sheet_name[:31], index=False)  # Sheet names must be <= 31 chars
                print(f"Results for {sf_folder} saved to {combined_excel_output_path}\n")
            except Exception as e:
                print(f"Error writing Excel file for {sf_folder}: {e}\n")
        else:
            print(f"No combined data to save for {sf_folder}.\n")


if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    print(f"Starting processing with base path: {base_path}\n")
    main(base_path)
    print("Processing completed.")
