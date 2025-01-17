import psycopg2
import time
import multiprocessing as mp
from functools import partial

# Database connection parameters
DB_PARAMS = {
    "dbname": "noaa_ais",
    "user": "ruixin",
    "password": "ruixin123",
    "host": "localhost",
    "port": "5432"
}

# Headers for dynamic and static tables
DYNAMIC_HEADERS = ["mmsi", "time", "longitude", "latitude", "rot", "sog", "cog", "heading", "maneuver", "utc_second", "source"]

STATIC_HEADERS = ["mmsi", "time", "vessel_name", "ship_type", "call_sign", "imo", "dim_bow", "dim_stern", "dim_port", "dim_star", "draught", 
                  "destination", "ais_version", "fixing_device", "eta_month", "eta_day", "eta_hour", "eta_minute", "source"]

AGGREGATE_HEADERS = ["mmsi", "imo", "vessel_name", "ship_type", "call_sign", "dim_bow", "dim_stern", "dim_port", "dim_star", "draught", 
                     "destination", "eta_month", "eta_day", "eta_hour", "eta_minute"]


# Connect to the database
def connect_to_db():
    return psycopg2.connect(**DB_PARAMS)

# Rename columns and convert timestamps to epoch time
def transform_table_columns(conn, table_name):
    with conn.cursor() as cur:
        # Update the timestamp column to epoch time
        cur.execute(f"""
            UPDATE {table_name}
            SET "basedatetime" = EXTRACT(EPOCH FROM "basedatetime"::timestamp)::BIGINT;
        """)
        
        # Mapping column names
        rename_map = {
            "basedatetime": "time",
            "lat": "latitude",
            "lon": "longitude",
            "vesselname": "vessel_name",
            "callsign": "call_sign",
            "vesseltype": "ship_type",
            "draft": "draught"
        }
        for old_col, new_col in rename_map.items():
            cur.execute(f'ALTER TABLE {table_name} RENAME COLUMN "{old_col}" TO "{new_col}";')
            
        conn.commit()
    

# Split a table into dynamic and static tables
def split_table(conn, original_table, YEAR, MONTH):
    dynamic_table = f"ais_{YEAR}{MONTH}_dynamic"
    static_table = f"ais_{YEAR}{MONTH}_static"
    aggregate_table = f"static_{YEAR}{MONTH}_aggregate"

    with conn.cursor() as cur:
        # Get existing columns from original table
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{original_table}';
        """)
        available_columns = [row[0] for row in cur.fetchall()]
        
        # Create SELECT expressions for different tables
        dynamic_selects = [
            f'"{col}"' if col in available_columns 
            else f'cast(null as text) as "{col}"'  # Create empty column
            for col in DYNAMIC_HEADERS
        ]
        
        static_selects = [
            f'"{col}"' if col in available_columns 
            else f'cast(null as text) as "{col}"'  # Create empty column
            for col in STATIC_HEADERS
        ]

        aggregate_selects = [
            f'"{col}"' if col in available_columns 
            else f'cast(null as text) as "{col}"'  # Create empty column
            for col in AGGREGATE_HEADERS
        ]
        
        # Create dynamic table
        cur.execute(f"""
            CREATE TABLE {dynamic_table} AS
            SELECT {', '.join(dynamic_selects)}
            FROM {original_table};
        """)
        
        # Create static table
        cur.execute(f"""
            CREATE TABLE {static_table} AS
            SELECT DISTINCT {', '.join(static_selects)}
            FROM {original_table};
        """)
        
        # Create static aggregate table
        cur.execute(f"""
            CREATE TABLE {aggregate_table} AS
            SELECT DISTINCT {', '.join(aggregate_selects)}
            FROM {original_table};
        """)
        
        conn.commit()
    

# Process a single year-month combination
def process_single_table(year_month):
    year, month = year_month
    month_str = f"{month:02d}"
    original_table = f"ais_{year}_{month_str}"
    
    try:
        # Create a new connection for each process
        conn = connect_to_db()
        print(f"Processing table: {original_table}")
        
        # Transform columns
        transform_table_columns(conn, original_table)
        
        # Split into dynamic and static tables
        split_table(conn, original_table, year, month_str)
        
        print(f"Finished processing table for {year}_{month_str}.")
        
    except Exception as e:
        print(f"Error processing {original_table}: {str(e)}")
    finally:
        conn.close()


def process_tables():
    # Generate list of (year, month) tuples to process
    tasks = [(year, month) 
             for year in range(2010, 2024)
             for month in range(1, 13)]
    
    num_processes = 8
    
    print(f"Starting processing with {num_processes} processes")
    
    # Create a pool of workers
    with mp.Pool(processes=num_processes) as pool:
        # Map the process_single_table function to all tasks
        pool.map(process_single_table, tasks)
        

if __name__ == "__main__":
    mp.freeze_support()
    process_tables()
