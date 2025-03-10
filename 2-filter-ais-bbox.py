"""
Geographic filtering for AIS data files.
This script filters AIS CSV files to only include records within a specified bounding box.
"""

import argparse
import time
import os
import aisdb
from util import filter_by_bbox

def process_month_files(year: int, month: int, bbox: tuple, base_dir: str, output_dir: str) -> tuple:
    """
    Filter a month's worth of AIS data files by geographic bounding box.
    
    Args:
        year: Year to process
        month: Month to process
        bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat)
        base_dir: Base directory containing source files
        output_dir: Directory for filtered output files
        
    Returns:
        tuple: (List of filtered files, Processing time)
    """
    start_time = time.time()
    
    # Get all files for this month
    month_dir = f"{base_dir}/{year}{month:02d}"
    filepaths = aisdb.glob_files(month_dir, '.csv')
    filepaths = sorted([f for f in filepaths if f'{year}{month:02d}' in f])
    
    print(f"Found {len(filepaths)} files for {year}{month:02d}")
    
    # Create month-specific output directory
    month_output_dir = f"{output_dir}/{year}{month:02d}"
    os.makedirs(month_output_dir, exist_ok=True)
    
    # Filter the files
    filtered_files = filter_by_bbox(
        file_paths=filepaths,
        bbox=bbox,
        output_dir=month_output_dir,
        prefix=""  # No prefix needed since files are in their own directory
    )
    
    elapsed_time = time.time() - start_time
    print(f"Filtered {year}{month:02d}: {len(filtered_files)}/{len(filepaths)} files contain data in bounding box")
    print(f"Time taken: {elapsed_time:.2f} seconds")
    
    return filtered_files, elapsed_time

def main():
    parser = argparse.ArgumentParser(description='Filter AIS data by geographic bounding box')
    parser.add_argument('--start-year', type=int, default=2023, help='Start year')
    parser.add_argument('--end-year', type=int, default=2023, help='End year')
    parser.add_argument('--start-month', type=int, default=1, help='Start month')
    parser.add_argument('--end-month', type=int, default=2, help='End month')
    parser.add_argument('--base-dir', type=str, default='/slow-array/NOAA-unzip', help='Base directory for source files')
    parser.add_argument('--output-dir', type=str, default='/slow-array/NOAA-filtered', help='Output directory for filtered files')
    parser.add_argument('--min-lon', type=float, default=-77.36, help='Minimum longitude')
    parser.add_argument('--min-lat', type=float, default=36.02, help='Minimum latitude')
    parser.add_argument('--max-lon', type=float, default=-57.62, help='Maximum longitude')
    parser.add_argument('--max-lat', type=float, default=48.64, help='Maximum latitude')
    
    args = parser.parse_args()
    
    # Create the bounding box
    bbox = (args.min_lon, args.min_lat, args.max_lon, args.max_lat)
    
    # Create the output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Save a record of the bounding box used
    with open(f"{args.output_dir}/bbox_info.txt", 'w') as f:
        f.write(f"Bounding Box: {bbox}\n")
        f.write(f"Min Longitude: {args.min_lon}\n")
        f.write(f"Min Latitude: {args.min_lat}\n")
        f.write(f"Max Longitude: {args.max_lon}\n")
        f.write(f"Max Latitude: {args.max_lat}\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Process each month
    all_filtered_files = []
    total_start_time = time.time()
    
    for year in range(args.start_year, args.end_year + 1):
        for month in range(args.start_month, args.end_month + 1):
            filtered_files, _ = process_month_files(
                year=year,
                month=month,
                bbox=bbox,
                base_dir=args.base_dir,
                output_dir=args.output_dir
            )
            all_filtered_files.extend(filtered_files)
    
    total_time = time.time() - total_start_time
    
    # Write a summary file
    with open(f"{args.output_dir}/filtering_summary.txt", 'w') as f:
        f.write(f"Filtering completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total files filtered: {len(all_filtered_files)}\n")
        f.write(f"Total processing time: {total_time:.2f} seconds\n")
        f.write(f"Bounding box: {bbox}\n")
        f.write("\nFiltered files:\n")
        for file in all_filtered_files:
            f.write(f"- {file}\n")
    
    print(f"\nFiltering complete. {len(all_filtered_files)} files filtered.")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Filtered files saved to: {args.output_dir}")
    
    # Create a file listing all filtered files for easy loading
    with open(f"{args.output_dir}/filtered_files_list.txt", 'w') as f:
        for file in all_filtered_files:
            f.write(f"{file}\n")

if __name__ == "__main__":
    main()
