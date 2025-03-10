import os
import zipfile
import shutil
import time

# Define source and destination directories
source_root = "/slow-array/NOAA"
destination_root = "/slow-array/NOAA-unzip"

# Define the start and end dates for extraction (format: YYYYMM)
start_date = "202301"
end_date = "202302"

# Create a log file to record errors
log_file_path = os.path.join(destination_root, "extraction_errors.log")

# Function to log errors
def log_error(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")
    print(f"ERROR: {message}")

# Create parent directory for log file if it doesn't exist
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# Counter for tracking progress
total_files = 0
successful_files = 0
failed_files = 0

try:
    # Iterate through subdirectories in the NOAA directory
    for year_month in sorted(os.listdir(source_root)):
        source_dir = os.path.join(source_root, year_month)
        destination_dir = os.path.join(destination_root, year_month)
        
        # Check if the directory exists, is a folder, and is within our date range
        if os.path.isdir(source_dir) and start_date <= year_month <= end_date:
            try:
                # Ensure the destination directory exists
                os.makedirs(destination_dir, exist_ok=True)
                
                # Iterate through all files in the directory
                for file in os.listdir(source_dir):
                    if file.endswith(".zip"):
                        zip_path = os.path.join(source_dir, file)
                        total_files += 1
                        
                        try:
                            print(f"Extracting {zip_path} to {destination_dir}")
                            
                            # Extract the ZIP file with error handling
                            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                                # Test the integrity of the zip file first
                                test_result = zip_ref.testzip()
                                if test_result is not None:
                                    # The zip file has at least one bad file
                                    error_msg = f"Corrupted file in zip: {zip_path}, first bad file: {test_result}"
                                    log_error(error_msg)
                                    failed_files += 1
                                    continue
                                
                                # Extract all files
                                zip_ref.extractall(destination_dir)
                                successful_files += 1
                                
                        except zipfile.BadZipFile as e:
                            error_msg = f"Bad zip file: {zip_path} - Error: {str(e)}"
                            log_error(error_msg)
                            failed_files += 1
                        except Exception as e:
                            error_msg = f"Error extracting {zip_path} - Error: {str(e)}"
                            log_error(error_msg)
                            failed_files += 1
            
            except Exception as e:
                error_msg = f"Error processing directory {source_dir} - Error: {str(e)}"
                log_error(error_msg)
    
    # Print summary
    print("\nExtraction Summary:")
    print(f"Total zip files processed: {total_files}")
    print(f"Successfully extracted: {successful_files}")
    print(f"Failed to extract: {failed_files}")
    print(f"See {log_file_path} for detailed error information")
    print("Unzipping complete!")

except Exception as e:
    log_error(f"Critical error in main process: {str(e)}")
    print("Unzipping process terminated due to critical error!")
