import os
from glob import glob
from concurrent.futures import ProcessPoolExecutor
def remove_duplicates_python(file_path):
    """
    Remove duplicate rows from a CSV file, keeping the header.
    This function reads the file line by line to handle large files efficiently.
    """
    seen = set()
    temp_file_path = file_path + ".tmp"

    try:
        with open(file_path, 'r') as infile:
            with open(temp_file_path, 'w') as outfile:
                header = next(infile)
                outfile.write(header)
                seen.add(header.strip())

                for line in infile:
                    if line.strip() not in seen:
                        outfile.write(line)
                        seen.add(line.strip())

        # Ensure the temporary file is not empty before replacing the original file
        if os.path.getsize(temp_file_path) > 0:
            os.replace(temp_file_path, file_path)
            print(f"Processed and updated {file_path}")
        else:
            print(f"Temporary file {temp_file_path} is empty. Original file not replaced.")
            os.remove(temp_file_path)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def process_files_in_parallel(directory):
    """
    Process CSV files in the specified directory in parallel, removing duplicates.
    """
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist.")
        return

    csv_files = glob(os.path.join(directory, "*.csv"))

    if not csv_files:
        print(f"No CSV files found in directory '{directory}'.")
        return

    max_workers = 2  # Adjust based on system capacity

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        executor.map(remove_duplicates_python, csv_files)


if __name__ == "__main__":
    directory = 'merged/'
    process_files_in_parallel(directory)
