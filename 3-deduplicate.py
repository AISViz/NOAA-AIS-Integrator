from concurrent.futures import ProcessPoolExecutor
from glob import glob

import subprocess
import tempfile
import os


def remove_duplicates_with_awk(file_path):
    """
    Use awk to remove duplicate rows from a CSV file, keeping the header.
    This function creates a temporary file securely for storing intermediate results.
    """
    with tempfile.NamedTemporaryFile(delete=False, mode='w', dir=os.path.dirname(file_path)) as temp_file:
        temp_file_path = temp_file.name

    # Construct the awk command to filter duplicates, preserving the header
    awk_cmd = f"awk 'NR == 1 || !seen[$0]++' {file_path} > {temp_file_path}"

    try:
        # Execute the awk command
        subprocess.run(awk_cmd, shell=True, check=True, executable='/bin/bash')
        # Replace the original file with the processed temporary file
        os.rename(temp_file_path, file_path)
        print(f"Processed and updated {file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {file_path}: {e}")
        # Remove the temporary file in case of an error
        os.remove(temp_file_path)


def process_files_in_parallel(directory):
    """
    Process CSV files in the specified directory in parallel, removing duplicates.
    """
    csv_files = glob(os.path.join(directory, "*.csv"))
    with ProcessPoolExecutor(max_workers=6) as executor:
        executor.map(remove_duplicates_with_awk, csv_files)


if __name__ == "__main__":
    directory = 'merged/'
    process_files_in_parallel(directory)
