import pandas as pd
import sys
import os

def get_average_times(file_path):
    # Load the Excel file for query execution times
    excel_data = pd.ExcelFile(file_path)
    avg_times = []
    
    # Iterate through each sheet and append the average time to an array
    for sheet in excel_data.sheet_names:
        data = pd.read_excel(file_path, sheet_name=sheet)
        avg_time = data['Average Time (s)'][0]
        avg_times.append((sheet, avg_time))
    return avg_times

def get_power_metrics_corrected(file_path):
    # Load the Excel file for power metrics
    excel_data = pd.ExcelFile(file_path)
    power_metrics = {}
    sheets_data = {sheet: excel_data.parse(sheet) for sheet in excel_data.sheet_names}

    # Process each sheet to find the 'Aggregate' row and extract 'Average Power'
    for sheet, data in sheets_data.items():
        standardized_sheet_name = sheet.replace(" ", "_")
        if 'Phase' in data.columns and 'Average Power' in data.columns:
            agg_avg_data = data[data['Phase'] == 'Aggregate']
            if not agg_avg_data.empty:
                avg_power = agg_avg_data['Average Power'].values[0]
                power_metrics[standardized_sheet_name] = avg_power
            else:
                power_metrics[standardized_sheet_name] = "N/A"
        else:
            power_metrics[standardized_sheet_name] = "N/A"
    return power_metrics

def combine_metrics(avg_times, power_metrics):
    # Array to store combined information with specified columns
    combined_data = []
    
    for sheet, avg_time in avg_times:
        avg_power = power_metrics.get(sheet, "N/A")
        
        # Calculate energy if avg_power is available
        if avg_power != "N/A":
            energy = avg_time * avg_power
        else:
            energy = "N/A"
        
        combined_data.append({
            "Frequency": sheet,
            "Average Time (s)": avg_time,
            "Average Power (W)": avg_power,
            "Energy (J)": energy
        })
    
    return combined_data

def save_to_excel(combined_data, output_path):
    # Convert combined data to DataFrame with specified columns
    combined_df = pd.DataFrame(combined_data)

    # Write to Excel
    combined_df.to_excel(output_path, sheet_name="Metrics", index=False)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        base_path = sys.argv[1]

    # Iterate over SF folders
    for sf_folder in os.listdir(base_path):
        sf_path = os.path.join(base_path, sf_folder)
        if os.path.isdir(sf_path) and sf_folder.startswith('SF'):
            # File paths for query execution times and power metrics
            query_execution_file = os.path.join(sf_path, f"query_execution_times_avg_{sf_folder}_combined.xlsx")
            power_metrics_file = os.path.join(sf_path, f"power_metrics_{sf_folder}_combined.xlsx")

            # Check if the required files exist
            if os.path.exists(query_execution_file) and os.path.exists(power_metrics_file):
                avg_times = get_average_times(query_execution_file)
                power_metrics = get_power_metrics_corrected(power_metrics_file)

                # Combine the metrics
                combined_data = combine_metrics(avg_times, power_metrics)

                # Print the combined information
                print(f"\nCombined Metrics for {sf_folder}:")
                for info in combined_data:
                    print(info)

                # Save the combined metrics to an Excel file
                output_path = os.path.join(sf_path, f"energy_metrics_{sf_folder}_combined.xlsx")
                save_to_excel(combined_data, output_path)
                print(f"Combined metrics saved to {output_path}")
            else:
                missing_files = []
                if not os.path.exists(query_execution_file):
                    missing_files.append("query_execution_times_avg_combined.xlsx")
                if not os.path.exists(power_metrics_file):
                    missing_files.append("power_metrics_combined.xlsx")
                print(f"Skipping {sf_folder} due to missing files: {', '.join(missing_files)}")
