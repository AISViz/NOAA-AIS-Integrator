import aisdb
import os
psql_conn_string = "postgresql://jinkun:123456@127.0.0.1:5432/aisviz"

filepaths = aisdb.glob_files('/meridian/AIS_archive/meridian/','.zip')
filepaths = sorted([f for f in filepaths if '202410' in f])
print(len(filepaths))

with aisdb.PostgresDBConn(libpq_connstring=psql_conn_string) as dbconn:
    aisdb.decode_msgs(filepaths,
                        dbconn=dbconn,
                        source='exactEarth_meopar',
                        verbose=True,
                        skip_checksum=False)
