# NOAA-AIS-Integrator
Acquisition and processing of AIS data from [Marine Cadastre](https://hub.marinecadastre.gov/pages/vesseltraffic) and integration into an AISdb-aligned database.

Script breakdown:

- `0-download-ais.py` downloads the AIS files by years specified in command-line arguments.
- `1-category-by-month.py` organizes downloaded daily AIS files by matching and grouping them into {year}{month} folders for storage.
- `1-zip2csv-xxxx-xxxx.py` *(deprecated)* unzips the AIS files. This script was created for processing different years, as the data files in early years are in Geodatabase format (`.gdb>
- `2-zip2csv-timerange.py` extracts the organized AIS files and saves them to new paths. Need to specify start and end months. Single thread processing.
- `2-zip2csv-extract-all.py` extracts the organized all AIS files and saves them to new paths. Multi-thread processing.
- `2-filter-ais-bbox.py` filters AIS data, retaining only records within a specified geographical bounding box and saving them to a new path.
- `3-deduplicate.py` *(deprecated)* removes duplicate rows from the merged AIS files.
- `3-psql-noaa.py` loads CSV files into PostgreSQL database with error loop.
- `3-sqlite-noaa.py` loads CSV files into SQLite database.
- `4-postgresql-database-noaa.py` *(simple)* loads CSV files into a PostgreSQL database.
- `4-postgresql-database.py` *(deprecated)* old version: CSV -> Spire CSV -> AISdb
- `util.py` contains a bounding box filtering function used by `2-filter-ais-bbox.py`. 


Example usage: 
```
python 0-download-ais.py --start-year 2023 --end-year 2024
python 1-category-by-month.py
python 2-zip2csv-extract-all.py
python 2-filter-ais-bbox.py (optional)
python 3-sqlite-noaa.py
```

