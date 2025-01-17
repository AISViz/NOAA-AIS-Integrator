import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Define paths
data_folder = 'data'
zip_folder = 'zip'

# Create the /zip/ directory if it doesn't exist
os.makedirs(zip_folder, exist_ok=True)


# Function to extract a single ZIP file
def extract_zip(zip_file):
    # Define the full path to the ZIP file
    zip_path = os.path.join(data_folder, zip_file)

    # Open the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extract all contents to the /zip/ folder
        zip_ref.extractall(zip_folder)

    return zip_file


# Get a list of all ZIP files in the /data/ directory
zip_files = [f for f in os.listdir(data_folder) if f.endswith('.zip')]

# Use ThreadPoolExecutor to extract files concurrently
with ThreadPoolExecutor() as executor:
    # Submit the extract_zip function for each ZIP file
    futures = [executor.submit(extract_zip, zip_file) for zip_file in zip_files]

    # Use tqdm to show progress
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Extracting ZIP files"):
        pass

print("All ZIP files have been extracted.")