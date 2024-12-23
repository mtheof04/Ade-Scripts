import pandas as pd
import sys
from pathlib import Path

def combine_metrics(query_execution_file, power_metrics_file, output_file):
    """
    Combines power metrics and query execution times into energy metrics.

    Parameters:
    - query_execution_file: Path to the query execution times Excel file.
    - power_metrics_file: Path to the power metrics Excel file.
    - output_file: Path where the combined energy metrics Excel file will be saved.
    """
    try:
        # Load the query execution times and power metrics
        query_execution_combined = pd.read_excel(query_execution_file, sheet_name=None)
        power_metrics_combined = pd.read_excel(power_metrics_file, sheet_name=None)
    except Exception as e:
        print(f"Error reading Excel files:\n{e}")
        return

    # Prepare the output data
    combined_data = []

    # Loop through sheets in the power metrics file
    for frequency in power_metrics_combined.keys():
        try:
            # Extract the frequency value from the sheet name (e.g., 'F1.0' -> 1.0)
            freq_value = float(frequency.lstrip('F'))
        except ValueError:
            print(f"  Skipping sheet '{frequency}' as it does not conform to expected naming.")
            continue

        # Load the power metrics and corresponding query execution times for this frequency
        power_df = power_metrics_combined[frequency]
        query_df = query_execution_combined.get(frequency)

        if query_df is None:
            print(f"  No matching query execution data for frequency '{frequency}'. Skipping.")
            continue

        # Merge the power metrics and query execution times on 'Phase' and 'Queries' columns
        merged_df = pd.merge(
            power_df[['Phase', 'Average Power']],
            query_df[['Queries', 'Avg Time (s)']].rename(columns={'Queries': 'Phase'}),
            on='Phase',
            how='inner'
        )

        if merged_df.empty:
            print(f"  No matching phases found for frequency '{frequency}'. Skipping.")
            continue

        # Calculate energy as Average Power * Avg Time (s)
        merged_df['Energy'] = merged_df['Average Power'] * merged_df['Avg Time (s)']

        # Add the frequency column
        merged_df['Frequency'] = freq_value

        # Rename columns for clarity
        merged_df = merged_df.rename(columns={
            'Phase': 'Phase',
            'Average Power': 'Workload Power',
            'Avg Time (s)': 'Execution Time'
        })

        # Reorder columns
        merged_df = merged_df[['Frequency', 'Phase', 'Workload Power', 'Execution Time', 'Energy']]

        # Append to combined data
        combined_data.append(merged_df)

    if combined_data:
        # Concatenate all combined data into a single DataFrame
        final_output_df = pd.concat(combined_data, ignore_index=True)

        try:
            # Save the result to an Excel file
            with pd.ExcelWriter(output_file) as writer:
                final_output_df.to_excel(writer, index=False, sheet_name="Combined Metrics")
            print(f"  Combined metrics saved to '{output_file}'.")
        except Exception as e:
            print(f"  Error saving the combined metrics:\n{e}")
    else:
        print("  No data combined. Please check the input files and sheet naming conventions.")

def process_base_path(base_path):
    """
    Processes all SF folders within the base path and combines their metrics.

    Parameters:
    - base_path: The root directory containing SF folders (e.g., SF100, SF300).
    """
    base = Path(base_path)

    if not base.exists() or not base.is_dir():
        print(f"The base path '{base_path}' does not exist or is not a directory.")
        return

    # Iterate over directories matching the pattern 'SF*' (e.g., SF100, SF300)
    sf_folders = [sf for sf in base.iterdir() if sf.is_dir() and sf.name.startswith('SF')]
    if not sf_folders:
        print(f"No 'SF' folders found in the base path '{base_path}'.")
        return

    for sf_folder in sf_folders:
        print(f"Processing folder: {sf_folder.name}")

        # Define file paths
        query_execution_file = sf_folder / "query_execution_times_avg_combined.xlsx"
        power_metrics_file = sf_folder / "power_metrics_combined.xlsx"
        output_file = sf_folder / "energy_metrics_combined.xlsx"

        # Check if the required input files exist
        missing_files = []
        if not query_execution_file.exists():
            missing_files.append(query_execution_file.name)
        if not power_metrics_file.exists():
            missing_files.append(power_metrics_file.name)

        if missing_files:
            print(f"  Missing file(s): {', '.join(missing_files)}. Skipping this folder.")
            continue

        # Combine metrics for the current SF folder
        combine_metrics(query_execution_file, power_metrics_file, output_file)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    process_base_path(base_path)
