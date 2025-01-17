import os
import argparse
import csv
from glob import glob


def get_month_from_filename(filename):
    """Extract the year and month from the filename."""
    return "_".join(filename.split('_')[1:3])  # Extracts 'YYYY_MM'


def merge_files_by_month(directory, output_directory):
    """Merge CSV files into separate files based on zones."""
    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    file_groups = {}
    # Group files by their month
    for filepath in glob(os.path.join(directory, '*.csv')):
        month = get_month_from_filename(os.path.basename(filepath))
        if month not in file_groups:
            file_groups[month] = []
        file_groups[month].append(filepath)

    # Process each group
    for month, files in file_groups.items():
        print(f"Processing month {month}...")
        output_file = os.path.join(output_directory, f"AIS_{month}.csv")
        headers = None

        with open(output_file, 'w', newline='') as outfile:
            writer = None

            for file in files:
                try:
                    with open(file, 'r', newline='') as infile:
                        reader = csv.reader(infile)
                        current_headers = next(reader, None)  # Safely get the headers

                        if current_headers is None:
                            print(f"File {file} is empty or has no headers, skipping.")
                            continue

                        if headers is None:
                            headers = current_headers
                            writer = csv.writer(outfile)
                            writer.writerow(headers)
                        elif headers != current_headers:
                            print(f"Header mismatch detected in month {month}. File: {file} skipped.")
                            continue

                        for row in reader:
                            writer.writerow(row)
                except Exception as e:
                    print(f"Error processing file {file}: {e}")


if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(description='Merge CSV files by month.')
    parser.add_argument('-i', type=str, required=True, help='The directory containing the CSV files to be merged.')
    parser.add_argument('-o', type=str, required=True, help='The directory where the merged files will be saved.')
    
    # Parse the arguments
    args = parser.parse_args()

    # Access the input and output directories from the arguments
    input_directory = args.i
    output_directory = args.o

    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    merge_files_by_month(input_directory, output_directory)
