import os
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re

# Define paths
base_dir = "/slow-array/NOAA"

# Ensure the base directory exists
if not os.path.exists(base_dir):
    print(f"Error: Directory {base_dir} does not exist.")
    exit()

# Get a list of all ZIP files in the base directory
zip_files = [f for f in os.listdir(base_dir) if f.endswith(".zip")]

# Regular expression to extract year and month from filenames
pattern = re.compile(r"(\d{4})_AIS_(\d{4})_(\d{2})_\d{2}\.zip")

# Function to extract a single ZIP file
def extract_zip(zip_file):
    zip_path = os.path.join(base_dir, zip_file)
    match = pattern.match(zip_file)

    if match:
        year = match.group(2)  # Extract year
        month = match.group(3)  # Extract month
        folder_name = os.path.join(base_dir, f"{year}{month}")

        # Create the destination folder if it doesn't exist
        os.makedirs(folder_name, exist_ok=True)

        try:
            # Open the ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract contents to the corresponding {year}{month} folder
                zip_ref.extractall(folder_name)

            # Remove the ZIP file after extraction
            os.remove(zip_path)
            return f"Extracted & Moved: {zip_file} → {folder_name}/"

        except zipfile.BadZipFile:
            return f"Error: {zip_file} is a corrupted ZIP file."

    return f"Skipped: {zip_file} (Filename does not match expected format)"


# Use ThreadPoolExecutor to extract files concurrently
with ThreadPoolExecutor() as executor:
    futures = {executor.submit(extract_zip, zip_file): zip_file for zip_file in zip_files}

    # Use tqdm to show progress
    for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting ZIP files"):
        print(future.result())

print("All ZIP files have been extracted and organized.")
