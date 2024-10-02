import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import os
import  pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import fiona


os.chdir('../')


print("Loading solar radiation data...")
solar_data = pq.read_table('data/raw/solar_radiation/nsrdb_arizona_data.parquet').to_pandas()
print("Solar radiation data loaded.")


print("Processing solar radiation data...")
solar_data['date'] = pd.to_datetime(solar_data['Timestamp']).dt.date
daily_solar = solar_data.groupby(['Latitude', 'Longitude', 'date']).agg({
    'GHI': 'sum',
    'DNI': 'sum',
    'DHI': 'sum'
}).reset_index()

annual_solar = daily_solar.groupby(['Latitude', 'Longitude']).agg({
    'GHI': 'mean',
    'DNI': 'mean',
    'DHI': 'mean'
}).reset_index()

annual_solar['geometry'] = annual_solar.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
annual_solar_gdf = gpd.GeoDataFrame(annual_solar, geometry='geometry', crs='EPSG:4326')
print("Solar radiation data processed.")

# Load electricity rates
print("Loading and processing electricity rates...")
electricity_rates = pd.read_csv('data/raw/electricity_rates/eia_arizona_rates.csv')
electricity_rates['date'] = pd.to_datetime(electricity_rates['Month'])
latest_12_months = electricity_rates.sort_values('date', ascending=False).head(12)
avg_rate = latest_12_months['Arizona : residential cents per kilowatthour'].mean() / 100  # Convert to dollars per kWh
print(f"Average electricity rate for the latest 12 months: ${avg_rate:.4f} per kWh")

# Load city boundaries
print("Loading city boundaries...")
cities = gpd.read_file('data/raw/city_boundaries/tl_2023_04_place.shp')
print("City boundaries loaded.")

# Define target cities
target_cities = ['Phoenix', 'Tucson', 'Tempe', 'Mesa', 'Flagstaff']



def calculate_solar_potential(row, avg_rate):
    # Constants
    panel_efficiency = 0.2  # 20% panel efficiency
    system_losses = 0.14  # 14% system losses
    azimuth = 180  # Assuming south-facing panels
    tilt = 20  # Assuming 20-degree tilt
    average_solar_hours_per_day = 6  # Approximate average for Arizona, adjust as needed

    # Calculate plane of array (POA) irradiance
    poa_direct = row['DNI'] * np.cos(np.radians(tilt))
    poa_diffuse = row['DHI'] * (1 + np.cos(np.radians(tilt))) / 2
    poa_reflected = row['GHI'] * 0.2 * (1 - np.cos(np.radians(tilt))) / 2  # Assuming ground reflectance of 0.2
    poa_total = poa_direct + poa_diffuse + poa_reflected

    # Calculate solar potential (kWh per year)
    solar_potential = row['roof_area'] * poa_total * panel_efficiency * (1 - system_losses) * 365 / 1000

    # Calculate estimated savings (only during solar hours)
    estimated_savings = solar_potential * avg_rate * (average_solar_hours_per_day / 24)

    return pd.Series({
        'annual_solar_potential_kwh': solar_potential,
        'annual_estimated_savings_usd': estimated_savings
    })


def process_city_buildings(city_name, city_boundary, annual_solar_gdf, avg_rate):
    print(f"Processing {city_name}...")

    chunksize = 100000
    city_buildings = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs='EPSG:4326')

    with fiona.open('data/raw/building_footprints/Arizona.geojson', 'r') as src:
        for i in tqdm(range(0, len(src), chunksize), desc="Processing building chunks"):
            chunk = gpd.read_file('data/raw/building_footprints/Arizona.geojson', rows=slice(i, i + chunksize))
            if chunk.crs != city_boundary.crs:
                chunk = chunk.to_crs(city_boundary.crs)
            chunk_in_city = gpd.sjoin(chunk, city_boundary, predicate='intersects') # within to intersects
            if not chunk_in_city.empty:
                city_buildings = pd.concat([city_buildings, chunk_in_city])

    if city_buildings.empty:
        print(f"No buildings found in {city_name}. Skipping...")
        return None


    if 'index_right' in city_buildings.columns:
        city_buildings = city_buildings.rename(columns={'index_right': 'index_right_build'})
    if 'index_right' in annual_solar_gdf.columns:
        annual_solar_gdf = annual_solar_gdf.rename(columns={'index_right': 'index_right_solar'})


    city_solar = gpd.sjoin_nearest(city_buildings, annual_solar_gdf, max_distance=0.045)


    city_solar = city_solar.drop(columns=['index_right'], errors='ignore')


    city_solar['roof_area'] = city_solar.geometry.to_crs(epsg=32612).area  # Assuming the CRS is in meters
    city_solar[['annual_solar_potential_kwh', 'annual_estimated_savings_usd']] = city_solar.apply(
        calculate_solar_potential, avg_rate=avg_rate, axis=1)


    city_solar['building_id'] = city_solar.index.astype(str)


    result = city_solar[
        ['building_id', 'geometry', 'roof_area', 'annual_solar_potential_kwh', 'annual_estimated_savings_usd']]
    result = result.rename(columns={'roof_area': 'roof_area_sqm'})

    # Simplify geometries and round values
    result['geometry'] = result['geometry'].simplify(tolerance=0.0001)
    result = result.round({'roof_area_sqm': 1, 'annual_solar_potential_kwh': 0, 'annual_estimated_savings_usd': 2})

    return result


for city_name in target_cities:
    city_boundary = cities[cities['NAME'] == city_name].to_crs('EPSG:4326')

    if city_boundary.empty:
        print(f"No boundary found for {city_name}. Skipping...")
        continue

    result = process_city_buildings(city_name, city_boundary, annual_solar_gdf, avg_rate)

    if result is not None and not result.empty:
        # Save processed data as GeoJSON
        # output_file = f'data/processed/{city_name.lower()}_solar_potential.geojson'
        output_file = f'data/processed/{city_name.lower()}_solar_potential.parquet'
        result['geometry'] = result['geometry'].apply(lambda geom: geom.wkb)
        table = pa.Table.from_pandas(result)
        pq.write_table(table, output_file)
        # result.to_file(output_file, driver='GeoJSON')
        print(f"Saved processed data for {city_name} to {output_file}")

        print(f"Total buildings processed: {len(result)}")
        print(f"Total annual solar potential: {result['annual_solar_potential_kwh'].sum():.2f} kWh")
        print(f"Total annual estimated savings: ${result['annual_estimated_savings_usd'].sum():.2f}")
    else:
        print(f"No data to save for {city_name}")

print("Data processing complete!")