import os
import aisdb
import pandas as pd
from glob import glob


def noaa2spire(csv_file, output_dir):
    df = pd.read_csv(csv_file, parse_dates=["BaseDateTime"])
    # Spire data headers
    list_of_headers_ = ["MMSI","Message_ID","Repeat_indicator","Time","Millisecond","Region","Country","Base_station","Online_data","Group_code","Sequence_ID","Channel","Data_length","Vessel_Name","Call_sign","IMO","Ship_Type","Dimension_to_Bow","Dimension_to_stern","Dimension_to_port","Dimension_to_starboard","Draught","Destination","AIS_version","Navigational_status","ROT","SOG","Accuracy","Longitude","Latitude","COG","Heading","Regional","Maneuver","RAIM_flag","Communication_flag","Communication_state","UTC_year","UTC_month","UTC_day","UTC_hour","UTC_minute","UTC_second","Fixing_device","Transmission_control","ETA_month","ETA_day","ETA_hour","ETA_minute","Sequence","Destination_ID","Retransmit_flag","Country_code","Functional_ID","Data","Destination_ID_1","Sequence_1","Destination_ID_2","Sequence_2","Destination_ID_3","Sequence_3","Destination_ID_4","Sequence_4","Altitude","Altitude_sensor","Data_terminal","Mode","Safety_text","Non-standard_bits","Name_extension","Name_extension_padding","Message_ID_1_1","Offset_1_1","Message_ID_1_2","Offset_1_2","Message_ID_2_1","Offset_2_1","Destination_ID_A","Offset_A","Increment_A","Destination_ID_B","offsetB","incrementB","data_msg_type","station_ID","Z_count","num_data_words","health","unit_flag","display","DSC","band","msg22","offset1","num_slots1","timeout1","Increment_1","Offset_2","Number_slots_2","Timeout_2","Increment_2","Offset_3","Number_slots_3","Timeout_3","Increment_3","Offset_4","Number_slots_4","Timeout_4","Increment_4","ATON_type","ATON_name","off_position","ATON_status","Virtual_ATON","Channel_A","Channel_B","Tx_Rx_mode","Power","Message_indicator","Channel_A_bandwidth","Channel_B_bandwidth","Transzone_size","Longitude_1","Latitude_1","Longitude_2","Latitude_2","Station_Type","Report_Interval","Quiet_Time","Part_Number","Vendor_ID","Mother_ship_MMSI","Destination_indicator","Binary_flag","GNSS_status","spare","spare2","spare3","spare4"]
    # Create a new dataframe with the specified headers
    df_new = pd.DataFrame(columns=list_of_headers_)

    # Populate the new dataframe with formatted data from the original dataframe
    df_new['Time'] = pd.to_datetime(df['BaseDateTime']).dt.strftime('%Y%m%d_%H%M%S')
    df_new['Latitude'] = df['LAT']
    df_new['Longitude'] = df['LON']
    df_new['Vessel_Name'] = df['VesselName']
    df_new['Call_sign'] = df['CallSign']
    df_new['Ship_Type'] = df['VesselType'].fillna(0).astype(int)
    df_new['Navigational_status'] = df['Status']
    df_new['Draught'] = df['Draft']
    df_new['Message_ID'] = 1  # Mark all messages as dynamic by default
    df_new['Millisecond'] = 0

    # Transfer additional columns from the original dataframe, if they exist
    for col_n in df_new:
        if col_n in df.columns:
            df_new[col_n] = df[col_n]

    # Extract static messages for each unique vessel
    filtered_df = df_new[df_new['Ship_Type'].notnull() & (df_new['Ship_Type'] != 0)]
    filtered_df = filtered_df.drop_duplicates(subset='MMSI', keep='first')
    filtered_df = filtered_df.reset_index(drop=True)
    filtered_df['Message_ID'] = 5  # Mark these as static messages

    # Merge dynamic and static messages into a single dataframe
    df_new = pd.concat([filtered_df, df_new])

    # Get the base filename from the input path
    base_filename = os.path.basename(csv_file)
    output_filename = base_filename.replace('.csv', '_aisdb.csv')

    # Save the final dataframe to a CSV file using the input filename
    df_new.to_csv(os.path.join(output_dir, output_filename), index=False, quoting=1)


def db_connection():
    # psql connection string
    USER = 'ruixin'
    PASSWORD = 'ruixin123'
    ADDRESS = '127.0.0.1'
    PORT = 5432
    DBNAME = 'noaa'  # Please change this to your database name, ensure it exists
    return f"postgresql://{USER}:{PASSWORD}@{ADDRESS}:{PORT}/{DBNAME}"


def load_data_to_db(psql_conn_string):
    filepaths = aisdb.glob_files('merged/','.csv')
    print(f'Number of files: {len(filepaths)}')

    with aisdb.PostgresDBConn(libpq_connstring=psql_conn_string) as dbconn:
        aisdb.decode_msgs(filepaths,
                        dbconn=dbconn,
                        source='NOAA',
                        verbose=True,
                        skip_checksum=False)
        

def main():
    try:
        # Directory containing the merged CSV files
        directory = 'merged/'
        output_dir = 'processed/'
        # Loop through CSV files and create tables and load data
        for csv_file in glob(os.path.join(directory, '*.csv')):        
            noaa2spire(csv_file, output_dir)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    
    psql_conn_string = db_connection()
    load_data_to_db(psql_conn_string)


if __name__ == "__main__":
    main()
