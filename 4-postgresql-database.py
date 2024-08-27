import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from glob import glob
import csv


def create_database(conn, dbname):
    """Create a new PostgreSQL database."""
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {dbname};")
        print(f"Database '{dbname}' created successfully.")


def create_table_from_csv(conn, table_name, csv_file):
    """Create a PostgreSQL table based on a CSV file structure."""
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)

        columns = ", ".join([f"{header} TEXT" for header in headers])

        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {table_name} ({columns});")
            conn.commit()
            print(f"Table '{table_name}' created successfully.")


def load_csv_to_table(conn, table_name, csv_file):
    """Load a CSV file into a PostgreSQL table."""
    with open(csv_file, 'r') as f:
        next(f)  # Skip the header row
        with conn.cursor() as cursor:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
            conn.commit()
            print(f"Data from '{csv_file}' loaded successfully into table '{table_name}'.")


def main():
    # Connection details for the initial PostgreSQL server (default database)
    initial_conn = psycopg2.connect(
        dbname="postgres",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    # Create a new database
    dbname = "your_new_database_name"
    create_database(initial_conn, dbname)
    initial_conn.close()

    # Connect to the new database
    conn = psycopg2.connect(
        dbname=dbname,
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    try:
        # Directory containing the merged CSV files
        directory = 'merged/'

        # Loop through CSV files and create tables and load data
        for csv_file in glob(os.path.join(directory, '*.csv')):
            # Extract table name from CSV file name
            table_name = os.path.basename(csv_file).split('.')[0]
            create_table_from_csv(conn, table_name, csv_file)
            load_csv_to_table(conn, table_name, csv_file)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
