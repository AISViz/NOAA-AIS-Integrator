import os
import zipfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re

# Define paths
base_dir = "/slow-array/NOAA"
dest_dir = "/slow-array/NOAA-unzip"

# Ensure the base directory exists
if not os.path.exists(base_dir):
    print(f"Error: Directory {base_dir} does not exist.")
    exit()

# Regular expression to match {year}{month} folder format
folder_pattern = re.compile(r"^\d{6}$")

# Function to extract a single ZIP file
def extract_zip(zip_path, output_folder):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
        return f"Extracted: {zip_path} → {output_folder}/"
    except zipfile.BadZipFile:
        return f"Error: {zip_path} is a corrupted ZIP file."

# Identify all {year}{month} folders in the base directory
year_month_folders = [f for f in os.listdir(base_dir) if folder_pattern.match(f) and os.path.isdir(os.path.join(base_dir, f))]

# Create a list of extraction tasks
tasks = []

with ThreadPoolExecutor() as executor:
    for ym_folder in year_month_folders:
        src_folder = os.path.join(base_dir, ym_folder)
        dest_folder = os.path.join(dest_dir, ym_folder)

        # Ensure the corresponding destination folder exists
        os.makedirs(dest_folder, exist_ok=True)

        # Get all ZIP files in the current year-month folder
        zip_files = [f for f in os.listdir(src_folder) if f.endswith(".zip")]

        for zip_file in zip_files:
            zip_path = os.path.join(src_folder, zip_file)
            tasks.append(executor.submit(extract_zip, zip_path, dest_folder))

    # Display progress bar
    for future in tqdm(as_completed(tasks), total=len(tasks), desc="Extracting ZIP files"):
        print(future.result())

print("All ZIP files have been extracted to NOAA-unzip.")

