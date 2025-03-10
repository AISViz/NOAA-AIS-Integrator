# util.py
import pandas as pd
import os
import csv
from typing import List, Tuple, Optional

def filter_by_bbox(
    file_paths: List[str],
    bbox: Tuple[float, float, float, float],
    output_dir: Optional[str] = None,
    prefix: str = "filtered_"
) -> List[str]:
    """
    Filter CSV files to only include rows that fall within a geographic bounding box.
    Handles encoding errors and CSV parsing issues by skipping problematic rows.
    
    Args:
        file_paths: List of CSV file paths to process
        bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat)
        output_dir: Directory to save filtered files. If None, uses same directory as input
        prefix: Prefix to add to filtered file names
        
    Returns:
        List of paths to the filtered CSV files
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    filtered_file_paths = []
    
    for file_path in file_paths:
        try:
            # Determine output path
            file_name = os.path.basename(file_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, f"{prefix}{file_name}")
            else:
                output_path = os.path.join(os.path.dirname(file_path), f"{prefix}{file_name}")
            
            # Track if we've written anything to this file
            file_has_data = False
            error_count = 0
            processed_count = 0
            filtered_count = 0
            
            try:
                # First attempt: Use pandas with error handling
                chunksize = 100000  # Adjust based on available memory
                first_chunk = True
                
                # Try reading with pandas, handling encoding and parsing errors
                try:
                    # For newer pandas versions
                    chunks = pd.read_csv(
                        file_path, 
                        chunksize=chunksize, 
                        on_bad_lines='skip',
                        encoding_errors='replace',
                        low_memory=False  # Prevent dtype warnings/errors
                    )
                except (TypeError, ValueError):
                    # Fall back for older pandas versions
                    chunks = pd.read_csv(
                        file_path, 
                        chunksize=chunksize, 
                        error_bad_lines=False,
                        encoding='utf-8', 
                        errors='replace',
                        low_memory=False
                    )
                
                for chunk in chunks:
                    try:
                        processed_count += len(chunk)
                        
                        # Handle columns with mixed types by converting to numeric
                        if 'LON' in chunk.columns and 'LAT' in chunk.columns:
                            # Convert to numeric, coercing errors to NaN
                            chunk['LON'] = pd.to_numeric(chunk['LON'], errors='coerce')
                            chunk['LAT'] = pd.to_numeric(chunk['LAT'], errors='coerce')
                            
                            # Drop rows with NaN coordinates
                            valid_coords = chunk.dropna(subset=['LON', 'LAT'])
                            error_count += len(chunk) - len(valid_coords)
                            chunk = valid_coords
                            
                            # Filter rows within the bounding box
                            mask = (
                                (chunk['LON'] >= min_lon) & 
                                (chunk['LON'] <= max_lon) & 
                                (chunk['LAT'] >= min_lat) & 
                                (chunk['LAT'] <= max_lat)
                            )
                            filtered_chunk = chunk[mask]
                            filtered_count += len(filtered_chunk)
                            
                            # Write to output file if we have data
                            if not filtered_chunk.empty:
                                mode = 'w' if first_chunk else 'a'
                                header = first_chunk
                                filtered_chunk.to_csv(output_path, mode=mode, index=False, header=header)
                                first_chunk = False
                                file_has_data = True
                        else:
                            print(f"Warning: LON or LAT columns not found in {file_path}")
                            error_count += len(chunk)
                    
                    except Exception as e:
                        # Log chunk-specific error and continue with next chunk
                        print(f"Error processing chunk in {file_path}: {str(e)}")
                        error_count += len(chunk)
                        continue
            
            except Exception as e:
                print(f"Pandas processing failed for {file_path}: {str(e)}")
                print(f"Falling back to line-by-line processing for {file_path}")
                
                # Second attempt: Process line by line using csv module
                try:
                    # First, determine the header
                    with open(file_path, 'r', errors='replace') as f:
                        header_line = f.readline().strip()
                        header = next(csv.reader([header_line]))
                    
                    # Find LON and LAT column indices
                    try:
                        lon_idx = header.index('LON')
                        lat_idx = header.index('LAT')
                    except ValueError:
                        print(f"Could not find LON or LAT columns in {file_path}")
                        continue
                    
                    # Process file line by line
                    first_write = True
                    with open(file_path, 'r', errors='replace') as f:
                        # Skip header as we already read it
                        next(f)
                        
                        # Parse and process each line
                        reader = csv.reader(f)
                        for row_num, row in enumerate(reader, 2):  # Start from 2 as we skipped header
                            try:
                                processed_count += 1
                                
                                # Skip rows with incorrect number of fields
                                if len(row) != len(header):
                                    error_count += 1
                                    continue
                                
                                # Extract and validate coordinates
                                try:
                                    lon = float(row[lon_idx])
                                    lat = float(row[lat_idx])
                                except (ValueError, IndexError):
                                    error_count += 1
                                    continue
                                
                                # Check if within bounding box
                                if (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
                                    filtered_count += 1
                                    
                                    # Write to output file
                                    mode = 'w' if first_write else 'a'
                                    with open(output_path, mode, newline='') as out_f:
                                        writer = csv.writer(out_f)
                                        if first_write:
                                            writer.writerow(header)
                                            first_write = False
                                        writer.writerow(row)
                                        file_has_data = True
                                
                                # Log progress periodically
                                if row_num % 1000000 == 0:
                                    print(f"Processed {row_num} rows in {file_path}")
                                    
                            except Exception as e:
                                # Skip problematic rows
                                error_count += 1
                                if row_num % 1000000 == 0:
                                    print(f"Error at row {row_num} in {file_path}: {str(e)}")
                    
                except Exception as e:
                    print(f"Line-by-line processing failed for {file_path}: {str(e)}")
            
            # Only add to filtered_file_paths if the file contains data
            if file_has_data and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                filtered_file_paths.append(output_path)
                print(f"Successfully filtered {file_path}: {filtered_count}/{processed_count} rows in bounding box, {error_count} skipped rows")
            elif os.path.exists(output_path):
                # Remove empty files
                os.remove(output_path)
                print(f"No data in bounding box for {file_path}")
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return filtered_file_paths

def clean_tmp_folders():
    """Remove all /tmp/tmp* directories before retrying."""
    import glob
    import shutil
    
    tmp_dirs = glob.glob('/tmp/tmp*')
    for tmp_dir in tmp_dirs:
        if os.path.isdir(tmp_dir):
            try:
                shutil.rmtree(tmp_dir)
                print(f"Removed temporary directory: {tmp_dir}")
            except Exception as e:
                print(f"Error removing {tmp_dir}: {e}")
