from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

import pandas as pd
import subprocess
import zipfile
import shutil
import os
import geopandas as gpd
import fiona


def convert_gdb_to_csv(zip_file, output_folder="csv/"):
    """
    Converts a file in GDB format to CSV format.

    :param zip_file: The path to the GDB zip file.
    :param output_folder: The folder where the CSV files will be saved. Default is "csv/".
    :return: None
    """
    gdb_path = unzip_into_directory(zip_file)  # export and return the path

    os.makedirs(output_folder, exist_ok=True)
    
    layers = fiona.listlayers(gdb_path)

    file_identifier = gdb_path.split('/')[1].split('.')[0]

    for layer in layers:
        try:
            # Read the layer using GeoPandas
            gdf = gpd.read_file(gdb_path, layer=layer)
            
            # Extract X (Longitude) and Y (Latitude) coordinates from the geometry column
            if 'geometry' in gdf.columns:
                gdf['X'] = gdf.geometry.x  # Longitude
                gdf['Y'] = gdf.geometry.y  # Latitude

            # Define CSV file path
            csv_path = os.path.join(output_folder, f"{layer}.csv")

            # Export to CSV
            gdf.to_csv(csv_path, index=False)

        except Exception as e:
            print(f"Failed to process layer {file_identifier}_{layer}: {e}")
        
    join_files(file_identifier)  # join the multiple layers into a single shared file format


def join_files(file_suffix, output_folder="unified/", csv_path="csv/"):
    """
    Joins multiple CSV files into a single file.

    :param file_suffix: The suffix of the files to be joined.
    :param output_folder: The folder where the joined file will be saved. Default is "unified/".
    :param csv_path: The path where the CSV files are located. Default is "csv/".
    :return: None
    """
    os.makedirs(output_folder, exist_ok=True)

    broadcast_df = pd.read_csv(f'{csv_path}Broadcast.csv',
                               dtype={'MMSI': str, 'VoyageID': str}, low_memory=False, engine="c")
    voyage_df = pd.read_csv(f'{csv_path}Voyage.csv',
                            dtype={'VoyageID': str, 'MMSI': str}, low_memory=False, engine="c")
    vessel_df = pd.read_csv(f'{csv_path}Vessel.csv',
                            dtype={'MMSI': str}, low_memory=False, engine="c")


    # >>> perform the join between Broadcast and Vessel on 'MMSI'
    broadcast_vessel_joined = pd.merge(broadcast_df, vessel_df, on='MMSI', how='left')
    # >>> perform the join between the result of the first join and Voyage on 'VoyageID'
    final_joined_df = pd.merge(broadcast_vessel_joined, voyage_df, on=['VoyageID', 'MMSI'], how='left')

    selected_final_df = final_joined_df[[
        # Select and reorder the columns in the final joined DataFrame
        'MMSI', 'BaseDateTime', 'Y', 'X', 'SOG', 'COG', 'Heading',
        'Name', 'IMO', 'CallSign', 'VesselType', 'Status',
        'Length', 'Width', 'Draught', 'Cargo'
    ]]

    # Renaming files to match more recent naming system
    renamed_final_df = selected_final_df.rename(
        columns={
            'Y': 'LAT',
            'X': 'LON',
            'Draught': 'Draft',
            'Name': 'VesselName'
        }
    )
    # Save the final DataFrame to a new CSV file
    renamed_final_df.to_csv(f'{output_folder}/{file_suffix}_UNIFIED.csv', index=False)


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
                        # Cleanup: Remove any intermediate directories left behind after moving the .gdb folder
                        if root != output_folder:
                            shutil.rmtree(root)
                        break  # Exit after finding the first .gdb folder

    return gdb_folder_full_path


def process_directory(curr_directory="data/"):
    """
    Process the given directory to convert GDB files to CSV files using multi-threading.

    :param curr_directory: The path of the current directory (default="zip/")
    :return: None
    """
    with ProcessPoolExecutor() as executor:
        futures = []  # store the results of the threads
        for zip_file in [os.path.join(curr_directory, d) for d in os.listdir(curr_directory) if d.endswith('.zip')]:
            futures.append(executor.submit(convert_gdb_to_csv, zip_file))
            # convert_gdb_to_csv(zip_file)
            # exit(1)
        for _ in tqdm(as_completed(futures), total=len(futures)):
            pass  # progress tracking


if __name__ == "__main__":
    process_directory()
    # Fix a filename to match the naming system of other files
    # os.rename("unified/May_Zone_18_UNIFIED.csv", "unified/Zone18_2009_05_UNIFIED.csv")
