import os
import sys
import pandas as pd

# Determine the base path
if len(sys.argv) > 1:
    base_path = sys.argv[1]

try:
    # Search for folders starting with "SF"
    sf_folders = [folder for folder in os.listdir(base_path) if folder.startswith('SF')]

    for sf_folder in sf_folders:
        sf_path = os.path.join(base_path, sf_folder)
        
        # File processing for both metrics
        for metric_type in ['power_metrics', 'energy_metrics']:
            file_name = f"{metric_type}_combined.xlsx"
            file_path = os.path.join(sf_path, file_name)

            if os.path.isfile(file_path):
                # Load and sort the Excel file
                excel_data = pd.ExcelFile(file_path)
                sorted_sheets = {}
                for sheet_name in excel_data.sheet_names:
                    data = excel_data.parse(sheet_name)
                    
                    # Determine the column to sort by
                    sort_column = None
                    if metric_type == "power_metrics" and "Overall Average Power (Watts)" in data.columns:
                        sort_column = "Overall Average Power (Watts)"
                    elif metric_type == "energy_metrics" and "Energy (Joules)" in data.columns:
                        sort_column = "Energy (Joules)"
                    
                    if sort_column:
                        sorted_sheets[sheet_name] = data.sort_values(by=sort_column, ascending=True)
                    else:
                        sorted_sheets[sheet_name] = data

                # Save sorted data to the same folder with the new naming format
                output_file_path = os.path.join(
                    sf_path, f"{metric_type}_sorted_combined.xlsx"
                )
                with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                    for sheet_name, data in sorted_sheets.items():
                        data.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Sorted file saved: {output_file_path}")
            else:
                print(f"File not found in folder {sf_folder}: {file_name}")
except FileNotFoundError:
    print(f"Base path not found: {base_path}")
