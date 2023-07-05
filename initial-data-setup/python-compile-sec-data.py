import os
import zipfile
import csv

# Define the path to the directory containing the zip files
zip_directory = "C:\Temp\SEC RAW DATA"

# Define the output CSV file path
output_csv = "C:\Temp\All-FTDData-Python.csv"

# Create an empty list to store the extracted file paths
file_paths = []

# Extract all the zip files in the directory
for root, dirs, files in os.walk(zip_directory):
    for file in files:
        if file.endswith(".zip"):
            zip_path = os.path.join(root, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(zip_directory)
                extracted_files = zip_ref.namelist()
                for extracted_file in extracted_files:
                    file_path = os.path.join(zip_directory, extracted_file)
                    file_paths.append(file_path)

# Convert text files to CSV and combine them into a single large file
with open(output_csv, 'w', newline='') as output_file:
    writer = csv.writer(output_file)
    
    # Write the CSV header
    writer.writerow(['SETTLEMENT DATE', 'CUSIP', 'SYMBOL', 'QUANTITY (FAILS)', 'DESCRIPTION', 'PRICE'])
    
    for file_path in file_paths:
        with open(file_path, 'r') as input_file:
            lines = input_file.readlines()
            lines = lines[1:-2]
            
            for line in lines:
                if 'Trailer record count' in line:
                    continue
                data = line.strip().split('|')
                # Replace bad price values with '0'
                data[-1] = '0' if data[-1] == '.' else data[-1]
                writer.writerow(data)