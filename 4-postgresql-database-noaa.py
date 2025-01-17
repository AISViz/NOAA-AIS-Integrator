import aisdb

# psql connection string
USER = 'ruixin'
PASSWORD = 'ruixin123'
ADDRESS = '127.0.0.1'
PORT = 5432
DBNAME = 'noaa'
psql_conn_string = f"postgresql://{USER}:{PASSWORD}@{ADDRESS}:{PORT}/{DBNAME}"

filepaths = aisdb.glob_files('merged/','.csv')
filepaths = sorted([f for f in filepaths if '202410' in f])
print(f'Number of files: {len(filepaths)}')

with aisdb.PostgresDBConn(libpq_connstring=psql_conn_string) as dbconn:
    aisdb.decode_msgs(filepaths,
                    dbconn=dbconn,
                    source='NOAA',
                    verbose=True,
                    skip_checksum=False)
    