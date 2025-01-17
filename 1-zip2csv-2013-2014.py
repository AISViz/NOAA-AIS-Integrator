from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from contextlib import contextmanager
from tqdm import tqdm
import gc
import pandas as pd
import subprocess
import logging
import zipfile
import shutil
import os
import geopandas as gpd
import fiona

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_file_with_retry(zip_file, max_retries=3, delay=5):
    """
    Process a single file with retry logic.
    
    :param zip_file: Path to the zip file to process
    :param max_retries: Maximum number of retry attempts
    :param delay: Delay between retries in seconds
    """
    for attempt in range(max_retries):
        try:
            convert_gdb_to_csv(zip_file)
            return  # Success, exit the function
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {zip_file}: {str(e)}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"All attempts failed for {zip_file}")

@contextmanager
def managed_gdf(gdf):
    """
    Context manager to ensure proper cleanup of GeoPandas dataframes.
    """
    try:
        yield gdf
    finally:
        del gdf
        gc.collect()

def convert_gdb_to_csv(zip_file, output_folder="csv/"):
    """
    Converts a file in GDB format to CSV format.

    :param zip_file: The path to the GDB zip file.
    :param output_folder: The folder where the CSV files will be saved. Default is "csv/".
    :return: None
    """
    gdb_path = unzip_into_directory(zip_file)
    if gdb_path is None:
        logging.warning(f"No valid GDB folder found in {zip_file}")
        return
        
    os.makedirs(output_folder, exist_ok=True)

    layers = fiona.listlayers(gdb_path)
    file_identifier = gdb_path.split('/')[1].split('.')[0]

    for layer in layers:
        try:
            with managed_gdf(gpd.read_file(gdb_path, layer=layer)) as gdf:
            
                if 'geometry' in gdf.columns:
                    gdf['X'] = gdf.geometry.x
                    gdf['Y'] = gdf.geometry.y
    
                csv_path = os.path.join(output_folder, f"{file_identifier}_{layer}.csv")
                
                # Write the DataFrame to CSV in chunks
                chunk_size = 100000  # Adjust based on available memory
                for i in range(0, len(gdf), chunk_size):
                    chunk = gdf.iloc[i:i+chunk_size]
                    mode = 'w' if i == 0 else 'a'
                    chunk.to_csv(csv_path, index=False, mode=mode, header=(i == 0))
            # # Recycle GeoDataframe after processing
            # del gdf
            # gc.collect()
            
        except Exception as e:
            print(f"Failed to process layer {file_identifier}_{layer}: {e}")

    join_files(file_identifier)
    shutil.rmtree(gdb_path)

def join_files(file_suffix, output_folder="unified/", csv_path="csv/"):
    """
    Joins multiple CSV files into a single file.

    :param file_suffix: The suffix of the files to be joined.
    :param output_folder: The folder where the joined file will be saved. Default is "unified/".
    :param csv_path: The path where the CSV files are located. Default is "csv/".
    :return: None
    """
    os.makedirs(output_folder, exist_ok=True)

    dfs = []
    for csv_file in [f'{csv_path}{file_suffix}_{file_suffix}_{name}.csv' for name in ['Broadcast', 'Voyage', 'Vessel']]:
        try:
            dfs.append(pd.read_csv(csv_file, dtype={'MMSI': str, 'VoyageID': str}, low_memory=False, engine="c"))
        except Exception as e:
            print(f"Failed to read CSV file {csv_file}: {e}")

    try:
        broadcast_vessel_joined = pd.merge(dfs[0], dfs[2], on='MMSI', how='left')
        final_joined_df = pd.merge(broadcast_vessel_joined, dfs[1], on=['VoyageID', 'MMSI'], how='left')

        selected_final_df = final_joined_df[[
            'MMSI', 'BaseDateTime', 'Y', 'X', 'SOG', 'COG', 'Heading',
            'Name', 'IMO', 'CallSign', 'VesselType', 'Status',
            'Length', 'Width', 'Draught', 'Cargo'
        ]]

        renamed_final_df = selected_final_df.rename(
            columns={
                'Y': 'LAT',
                'X': 'LON',
                'Draught': 'Draft',
                'Name': 'VesselName'
            }
        )

        renamed_final_df.to_csv(f'{output_folder}/{file_suffix}_UNIFIED.csv', index=False)
    except Exception as e:
        print(f"Failed to join data layers of {file_suffix}: {e}")


def unzip_into_directory(zip_path, output_folder="gdb/"):
    """
    Unzips the given ZIP file into the specified output folder. If the ZIP contains a folder that
    itself contains a .gdb folder, moves the .gdb folder up to the output folder. Returns the full
    path to the unzipped .gdb folder.

    :param zip_path: The path to the ZIP file.
    :param output_folder: The target folder for the extracted contents. Defaults to "gdb/".
    :return: The full path to the unzipped folder ending with .gdb.
    """
    gdb_folder_full_path = None  # Initialize variable to store the full path of the .gdb folder

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder)
        extracted_paths = zip_ref.namelist()

    # Identify the initial extraction path, assuming the ZIP contains a single top-level directory
    if extracted_paths:
        initial_folder_name = extracted_paths[0].split('/')[0]
        full_folder_path = os.path.join(output_folder, initial_folder_name)

        # Check if the extracted content itself is the .gdb folder
        if full_folder_path.endswith('.gdb'):
            gdb_folder_full_path = full_folder_path
        else:
            # Look for a .gdb folder inside the extracted content
            for root, dirs, files in os.walk(full_folder_path):
                for dir_name in dirs:
                    if dir_name.endswith('.gdb'):
                        gdb_folder_path = os.path.join(root, dir_name)
                        target_path = os.path.join(output_folder, dir_name)
                        if not os.path.exists(target_path):
                            shutil.move(gdb_folder_path, output_folder)
                            gdb_folder_full_path = target_path
                        else:
                            # Handle case where the target .gdb folder already exists in the output folder
                            gdb_folder_full_path = target_path
                        # Log the root folder before deleting to ensure it's the correct one
                        print(f"Cleaning up: {root}")
                        # Cleanup: Remove any intermediate directories left behind after moving the .gdb folder
                        if root != output_folder and os.path.exists(root):
                            shutil.rmtree(root)
    return gdb_folder_full_path


def process_directory(curr_directory="data/", max_workers=2):
    """
    Process the given directory to convert GDB files to CSV files using multi-processing.

    :param curr_directory: The path of the current directory (default="data/")
    :param max_workers: Maximum number of worker processes to use
    :return: None
    """
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        zip_files = [os.path.join(curr_directory, d) for d in os.listdir(curr_directory) if d.endswith('.zip')]
        for zip_file in zip_files:
            futures.append(executor.submit(process_file_with_retry, zip_file))
        
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred during processing: {e}")


if __name__ == "__main__":
    process_directory(max_workers=2)

# from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
# from tqdm import tqdm
# import gc
# import pandas as pd
# import subprocess
# import zipfile
# import shutil
# import os
# import geopandas as gpd
# import fiona


# def convert_gdb_to_csv(zip_file, output_folder="csv/"):
#     """
#     Converts a file in GDB format to CSV format.

#     :param zip_file: The path to the GDB zip file.
#     :param output_folder: The folder where the CSV files will be saved. Default is "csv/".
#     :return: None
#     """
#     gdb_path = unzip_into_directory(zip_file)  # export and return the path

#     os.makedirs(output_folder, exist_ok=True)
    
#     layers = fiona.listlayers(gdb_path)

#     file_identifier = gdb_path.split('/')[1].split('.')[0]

#     for layer in layers:
#         try:
#             # Read the layer using GeoPandas
#             gdf = gpd.read_file(gdb_path, layer=layer)
            
#             # Extract X (Longitude) and Y (Latitude) coordinates from the geometry column
#             if 'geometry' in gdf.columns:
#                 gdf['X'] = gdf.geometry.x  # Longitude
#                 gdf['Y'] = gdf.geometry.y  # Latitude

#             # Define CSV file path
#             csv_path = os.path.join(output_folder, f"{file_identifier}_{layer}.csv")

#             # Export to CSV and recycle the GeoPandas dataframe
#             gdf.to_csv(csv_path, index=False)
#             del gdf
#             gc.collect()

#         except Exception as e:
#             print(f"Failed to process layer {file_identifier}_{layer}: {e}")
        
#     join_files(file_identifier)  # join the multiple layers into a single shared file format
#     shutil.rmtree(gdb_path)


# def join_files(file_suffix, output_folder="unified/", csv_path="csv/"):
#     """
#     Joins multiple CSV files into a single file.

#     :param file_suffix: The suffix of the files to be joined.
#     :param output_folder: The folder where the joined file will be saved. Default is "unified/".
#     :param csv_path: The path where the CSV files are located. Default is "csv/".
#     :return: None
#     """
#     os.makedirs(output_folder, exist_ok=True)

#     broadcast_df = pd.read_csv(f'{csv_path}{file_suffix}_{file_suffix}_Broadcast.csv',
#                                dtype={'MMSI': str, 'VoyageID': str}, low_memory=False, engine="c")
#     voyage_df = pd.read_csv(f'{csv_path}{file_suffix}_{file_suffix}_Voyage.csv',
#                             dtype={'VoyageID': str, 'MMSI': str}, low_memory=False, engine="c")
#     vessel_df = pd.read_csv(f'{csv_path}{file_suffix}_{file_suffix}_Vessel.csv',
#                             dtype={'MMSI': str}, low_memory=False, engine="c")

#     try:
#         # >>> perform the join between Broadcast and Vessel on 'MMSI'
#         broadcast_vessel_joined = pd.merge(broadcast_df, vessel_df, on='MMSI', how='left')
#         # >>> perform the join between the result of the first join and Voyage on 'VoyageID'
#         final_joined_df = pd.merge(broadcast_vessel_joined, voyage_df, on=['VoyageID', 'MMSI'], how='left')
    
#     except Exception as e:
#         print(f"Failed to join data layers of {file_suffix}: {e}")


#     try:
#         selected_final_df = final_joined_df[[
#             # Select and reorder the columns in the final joined DataFrame
#             'MMSI', 'BaseDateTime', 'Y', 'X', 'SOG', 'COG', 'Heading',
#             'Name', 'IMO', 'CallSign', 'VesselType', 'Status',
#             'Length', 'Width', 'Draught', 'Cargo'
#         ]]

#         # Renaming files to match more recent naming system
#         renamed_final_df = selected_final_df.rename(
#             columns={
#                 'Y': 'LAT',
#                 'X': 'LON',
#                 'Draught': 'Draft',
#                 'Name': 'VesselName'
#             }
#         )
#     except Exception as e:
#         print(f"Failed to select and join prefered fields of {file_suffix}: {e}")
        
#     # Save the final DataFrame to a new CSV file
#     renamed_final_df.to_csv(f'{output_folder}/{file_suffix}_UNIFIED.csv', index=False)


# def unzip_into_directory(zip_path, output_folder="gdb/"):
#     """
#     Unzips the given ZIP file into the specified output folder. If the ZIP contains a folder that
#     itself contains a .gdb folder, moves the .gdb folder up to the output folder. Returns the full
#     path to the unzipped .gdb folder.

#     :param zip_path: The path to the ZIP file.
#     :param output_folder: The target folder for the extracted contents. Defaults to "gdb/".
#     :return: The full path to the unzipped folder ending with .gdb.
#     """
#     gdb_folder_full_path = None  # Initialize variable to store the full path of the .gdb folder

#     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#         zip_ref.extractall(output_folder)
#         extracted_paths = zip_ref.namelist()

#     # Identify the initial extraction path, assuming the ZIP contains a single top-level directory
#     if extracted_paths:
#         initial_folder_name = extracted_paths[0].split('/')[0]
#         full_folder_path = os.path.join(output_folder, initial_folder_name)

#         # Check if the extracted content itself is the .gdb folder
#         if full_folder_path.endswith('.gdb'):
#             gdb_folder_full_path = full_folder_path
#         else:
#             # Look for a .gdb folder inside the extracted content
#             for root, dirs, files in os.walk(full_folder_path):
#                 for dir_name in dirs:
#                     if dir_name.endswith('.gdb'):
#                         gdb_folder_path = os.path.join(root, dir_name)
#                         target_path = os.path.join(output_folder, dir_name)
#                         if not os.path.exists(target_path):
#                             shutil.move(gdb_folder_path, output_folder)
#                             gdb_folder_full_path = target_path
#                         else:
#                             # Handle case where the target .gdb folder already exists in the output folder
#                             gdb_folder_full_path = target_path
#                         # Log the root folder before deleting to ensure it's the correct one
#                         print(f"Cleaning up: {root}")
#                         # Cleanup: Remove any intermediate directories left behind after moving the .gdb folder
#                         if root != output_folder and os.path.exists(root):
#                             shutil.rmtree(root)
#     return gdb_folder_full_path


# def process_directory(curr_directory="data/"):
#     """
#     Process the given directory to convert GDB files to CSV files using multi-threading.

#     :param curr_directory: The path of the current directory (default="zip/")
#     :return: None
#     """
#     # with ProcessPoolExecutor(max_workers=2) as executor:
#     with ProcessPoolExecutor(max_workers=2) as executor:
#         futures = []  # store the results of the threads
        
#         zip_files = [os.path.join(curr_directory, d) for d in os.listdir(curr_directory) if d.endswith('.zip')]
#         for zip_file in zip_files:
#             futures.append(executor.submit(convert_gdb_to_csv, zip_file))
            
#         for future in tqdm(as_completed(futures), total=len(futures)):
#             try:
#                 future.result()  # This will raise any exception caught during processing
#             except Exception as e:
#                 print(f"Error occurred during processing: {e}")


# if __name__ == "__main__":
#     process_directory()