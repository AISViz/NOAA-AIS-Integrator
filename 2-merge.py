from glob import glob

import csv
import os


def get_file_type(filename):
    """ Extract the type from the filename. """
    return "_".join(filename.split('.')[0].split('_')[1:])


def merge_csv_files(directory, output_directory):
    """ Merge CSV files of the same type with matching headers. """
    file_groups = {}
    # Group files by their type
    for filepath in glob(os.path.join(directory, '*.csv')):
        file_type = get_file_type(os.path.basename(filepath))
        if file_type not in file_groups:
            file_groups[file_type] = []
        file_groups[file_type].append(filepath)

    # Process each group
    for file_type, files in file_groups.items():
        output_file = os.path.join(output_directory, f"{file_type}.csv")
        headers = None

        with open(output_file, 'w', newline='') as outfile:
            writer = None

            for file in files:
                with open(file, 'r', newline='') as infile:
                    reader = csv.reader(infile)
                    current_headers = next(reader)

                    if headers is None:
                        headers = current_headers
                        writer = csv.writer(outfile)
                        writer.writerow(headers)
                    elif headers != current_headers:
                        print(f"Header mismatch detected in {file_type} group. File: {file} skipped.")
                        continue

                    for row in reader:
                        writer.writerow(row)


if __name__ == "__main__":
    directory = 'unified/'  # Directory containing the CSV files
    output_directory = 'merged/'  # Output directory for merged files
    merge_csv_files(directory, output_directory)
