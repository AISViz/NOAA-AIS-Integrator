import aisdb
import time
import shutil
import glob
import os

dbpath = './marine_cadastre_NE_2023_Jan_Feb.db'

start_year = 2023
end_year = 2023
start_month = 1
end_month = 2

failed_batches = set() # store the failed files and loop through them later

overall_start_time = time.time()

def clean_tmp_folders():
    """Remove all /tmp/tmp* directories before retrying."""
    tmp_dirs = glob.glob('/tmp/tmp*')
    for tmp_dir in tmp_dirs:
        if os.path.isdir(tmp_dir):
            try:
                shutil.rmtree(tmp_dir)
                print(f"Removed temporary directory: {tmp_dir}")
            except Exception as e:
                print(f"Error removing {tmp_dir}: {e}")


def month_process(year: int, month: int) -> bool:
    print(f'Loading {year}{month:02d}')

    filepaths = aisdb.glob_files(f'/slow-array/NOAA-filtered/{year}{month:02d}', '.csv')
    filepaths = sorted([f for f in filepaths if f'{year}{month:02d}' in f])

    print(f'Number of files: {len(filepaths)}')

    try:
        with aisdb.SQLiteDBConn(dbpath=dbpath) as dbconn:
            aisdb.decode_msgs(filepaths,
                              dbconn=dbconn,
                              source='noaa',
                              verbose=True,
                              skip_checksum=True,
                              raw_insertion=True,
                              workers=6)
    except Exception as e:
        print(f'Error loading {year}{month:02d}: {e}')
        return False

    return True


for year in range(start_year, end_year + 1):
    for month in range(start_month, end_month + 1):
        month_start_time = time.time()
        # should grasp all the files 
        success = month_process(year, month)
        if not success:
            failed_batches.add((year, month))

        month_end_time = time.time()
        print(f'Time taken for {year}{month:02d}: {month_end_time - month_start_time:.2f} seconds')

overall_end_time = time.time()
print(f'Total execution time for the first pass: {overall_end_time - overall_start_time:.2f} seconds')

# retry loop for failed batches
max_retries = 3
retry_attempt = 0

while failed_batches and retry_attempt < max_retries:
    print(f'\nRetry attempt {retry_attempt + 1} for {len(failed_batches)} failed batches.')

    clean_tmp_folders() # clean up temp folders before retry

    current_failures = failed_batches.copy()  # copy failed set and clean for reprocessing
    failed_batches.clear()

    for year, month in current_failures:
        print(f'Retrying {year}{month:02d}')
        success = month_process(year, month)
        if not success:
            failed_batches.add((year, month))

    # wait before next retry attempt
    if failed_batches:
        print(f'{len(failed_batches)} batches still failing after attempt {retry_attempt + 1}. Waiting before retry...')
        time.sleep(10)  # sleep for 10 seconds
    retry_attempt += 1

if failed_batches:
    print(f'\nThe following batches failed after {max_retries} attempts: {failed_batches}')
else:
    print('\nAll batches processed successfully after retries.')

print('Processing complete.')
