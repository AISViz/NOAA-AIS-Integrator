import os
import zipfile

# Define source and destination directories
source_root = "/slow-array/ruixin/NOAA"
destination_root = "/slow-array/ruixin/NOAA-unzip"

# Iterate through subdirectories in the NOAA directory
for year_month in os.listdir(source_root):
    source_dir = os.path.join(source_root, year_month)
    destination_dir = os.path.join(destination_root, year_month)

    # Check if the directory exists and is a folder
    if os.path.isdir(source_dir):
        # Ensure the destination directory exists
        os.makedirs(destination_dir, exist_ok=True)

        # Iterate through all files in the directory
        for file in os.listdir(source_dir):
            if file.endswith(".zip"):
                zip_path = os.path.join(source_dir, file)
                print(f"Extracting {zip_path} to {destination_dir}")

                # Extract the ZIP file
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(destination_dir)

print("Unzipping complete!")
