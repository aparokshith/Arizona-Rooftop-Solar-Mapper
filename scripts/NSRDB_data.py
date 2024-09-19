import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import os
from multiprocessing import Pool


def process_file(file_path):
    # Extract latitude and longitude from filename
    filename = os.path.basename(file_path)
    lat, lon = filename.split('_')[1:3]

    # Read CSV file, skipping header rows
    df = pd.read_csv(file_path, skiprows=2)

    # Add latitude and longitude columns
    df['Latitude'] = float(lat)
    df['Longitude'] = float(lon)

    # Create datetime column
    df['Timestamp'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour', 'Minute']])

    # Select relevant columns
    columns_to_keep = ['Timestamp', 'Latitude', 'Longitude', 'DHI', 'DNI', 'GHI']
    df = df[columns_to_keep]

    return df


def process_all_files(directory):
    all_files = glob.glob(os.path.join(directory, '*.csv'))

    # Use multiprocessing to speed up file processing
    with Pool() as pool:
        dfs = pool.map(process_file, all_files)

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    # Sort by timestamp and location
    combined_df = combined_df.sort_values(['Timestamp', 'Latitude', 'Longitude'])

    return combined_df


def save_to_parquet(df, output_file):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file, compression='snappy')


if __name__ == "__main__":
    input_directory = "NSRDB Raw"
    output_file = "processed_solar_data.parquet"

    print("Processing NSRDB data...")
    processed_data = process_all_files(input_directory)

    print("Saving processed data to Parquet file...")
    save_to_parquet(processed_data, output_file)

    print(f"Processing complete. Output saved to {output_file}")