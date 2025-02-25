import aisdb

# psql connection string
USER = 'ruixin'
PASSWORD = 'ruixin123'
ADDRESS = '127.0.0.1'
PORT = 5432
DBNAME = 'noaa'
psql_conn_string = f"postgresql://{USER}:{PASSWORD}@{ADDRESS}:{PORT}/{DBNAME}"

filepaths = aisdb.glob_files('/slow-array/NOAA/test/*', '.zip')
print(filepaths)
print(f'Number of files: {len(filepaths)}')

with aisdb.PostgresDBConn(libpq_connstring=psql_conn_string) as dbconn:
    try:
        aisdb.decode_msgs(filepaths,
                        dbconn=dbconn,
                        source='NOAA',
                        verbose=True,
                        skip_checksum=True,
                        raw_insertion=True,
                        workers=6,
                        timescaledb=True)
    except Exception as e:
        print(f'Error loading: {e}')
