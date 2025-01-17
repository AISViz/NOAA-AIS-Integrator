# NOAA-AIS-Integrator
Acquisition and processing of AIS data from [Marine Cadastre](https://hub.marinecadastre.gov/pages/vesseltraffic) and integration into an AISdb-aligned database.

Script breakdown:

- `0-download-ais.py` downloads the AIS files by years specified in command-line arguments. 
- `1-zip2csv-xxxx-xxxx.py` unzips the AIS files downloaded by `0-download-ais.py`. This script was created for processing different years, as the data files in early years are in Geodatabase format (`.gdb`) vs recent years in CSV format. Please use the correct script for the year range you are interested in.
- `2-merge.py` merges the AIS files by month.
- `3-deduplicate.py` removes duplicate rows from the merged AIS files.
- `4-postgresql-database.py` loads the deduplicated AIS files into a PostgreSQL database.



Example usage: 
```
python 0-download-ais.py --start_year 2020 --end_year 2021
python 1-zip2csv-2015-2023.py
python 2-merge.py -i /unzipped -o /merged
python 3-deduplicate.py 
python 4-postgresql-database.py (not recommended)
python 4-postgresql-database-noaa.py (recommended)
```

