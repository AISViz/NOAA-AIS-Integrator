import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from glob import glob
import csv
import argparse

def clean_csv(input_file):
    # Create a temporary file name in the same directory as the input file
    output_file = input_file + '.cleaned'

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        try:
            # Read the header to determine the expected number of columns
            header = next(reader)
            expected_number_of_columns = len(header)
            writer.writerow(header)
        except StopIteration:
            print(f"File '{input_file}' is empty or corrupted, skipping this file.")
            return

        for row in reader:
            if len(row) == expected_number_of_columns:
                writer.writerow(row)

    # Replace the original file with the cleaned one
    try:
        os.replace(output_file, input_file)
        print(f"Cleaned {input_file} successfully.")
    except Exception as e:
        print(f"Failed to replace original file: {e}")
        os.remove(output_file)  # Clean up the temporary file if something goes wrong


def create_database(conn, dbname):
    """Create a new PostgreSQL database if it doesn't already exist."""
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {dbname};")
            print(f"Database '{dbname}' created successfully.")
        else:
            print(f"Database '{dbname}' already exists.")


def create_table_from_csv(conn, table_name, csv_file):
    """Create a PostgreSQL table based on a CSV file structure."""
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)

        columns = ", ".join([f"{header} TEXT" for header in headers]) # This need to be changed as column data types varying

        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")
            conn.commit()
            print(f"Table '{table_name}' created successfully.")


def load_csv_to_table(conn, table_name, csv_file):
    """Load a CSV file into a PostgreSQL table."""
    with open(csv_file, 'r') as f:
        with conn.cursor() as cursor:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
            conn.commit()
            print(f"Data from '{csv_file}' loaded successfully into table '{table_name}'.")


def main():
    parser = argparse.ArgumentParser(description="Create a PostgreSQL database and import CSV files into it.")
    parser.add_argument('-dbname', required=True, help="The name of the PostgreSQL database to create.")
    parser.add_argument('-user', required=True, help="The PostgreSQL username.")
    parser.add_argument('-password', required=True, help="The PostgreSQL password.")
    parser.add_argument('-host', default="localhost", help="The PostgreSQL host (default: localhost).")
    parser.add_argument('-port', default="5432", help="The PostgreSQL port (default: 5432).")
    args = parser.parse_args()

    # Connect to the PostgreSQL server with the provided user credentials
    conn = psycopg2.connect(
        dbname="postgres",  # Connect to the default database to create a new one
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )

    # Create a new database if it doesn't exist
    create_database(conn, args.dbname)
    conn.close()

    # Connect to the new database
    conn = psycopg2.connect(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )

    try:
        # Directory containing the merged CSV files
        directory = 'merged/'

        # Loop through CSV files and create tables and load data
        for csv_file in glob(os.path.join(directory, '*.csv')):
            # Extract table name from CSV file name
            table_name = os.path.basename(csv_file).split('.')[0]
            # Clening up extra column(s) before loading the data
            clean_csv(csv_file)
            create_table_from_csv(conn, table_name, csv_file)
            load_csv_to_table(conn, table_name, csv_file)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
