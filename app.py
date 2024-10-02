import streamlit as st
import geopandas as gpd
import pyarrow.parquet as pq
import os
from shapely import wkb
import folium
from streamlit_folium import folium_static
from shapely.geometry import mapping

# Set page configuration
st.set_page_config(page_title="Arizona Rooftop Solar Potential Mapper", layout="wide")

# Constants
DATA_FOLDER = 'data/processed'
CHUNK_SIZE = 10000

# Title
st.title("Arizona Rooftop Solar Potential Mapper")


@st.cache_data
def get_available_cities():
    return sorted([f.split('_')[0].capitalize() for f in os.listdir(DATA_FOLDER) if f.endswith('.parquet')])


def load_city_data_in_chunks(city):
    file_path = os.path.join(DATA_FOLDER, f"{city.lower()}_solar_potential.parquet")
    parquet_file = pq.ParquetFile(file_path)

    for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
        df = batch.to_pandas()
        df['geometry'] = df['geometry'].apply(wkb.loads)
        yield gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")


def create_map(city):
    m = folium.Map(zoom_start=11)

    buildings = folium.FeatureGroup(name="Buildings")

    for chunk in load_city_data_in_chunks(city):
        for _, row in chunk.iterrows():
            folium.GeoJson(
                {
                    'type': 'Feature',
                    'geometry': mapping(row['geometry']),
                    'properties': {
                        'building_id': row['building_id'],
                        'roof_area_sqm': row['roof_area_sqm'],
                        'annual_solar_potential_kwh': row['annual_solar_potential_kwh'],
                        'annual_estimated_savings_usd': row['annual_estimated_savings_usd']
                    }
                },
                style_function=lambda x: {
                    'fillColor': 'blue',
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.7
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=['building_id', 'roof_area_sqm', 'annual_solar_potential_kwh',
                            'annual_estimated_savings_usd'],
                    aliases=['Building ID', 'Roof Area (sq m)', 'Annual Solar Potential (kWh)',
                             'Annual Estimated Savings ($)'],
                    style=(
                        "background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
                )
            ).add_to(buildings)

    buildings.add_to(m)
    folium.LayerControl().add_to(m)

    return m


# Sidebar for city selection
available_cities = get_available_cities()
city = st.sidebar.selectbox("Choose a city", available_cities)

# Create and display map
st.subheader(f"Solar Potential Map - {city}")
map_obj = create_map(city)
folium_static(map_obj)

# Calculate and display basic information
total_buildings = 0
total_solar_potential = 0
total_estimated_savings = 0

for chunk in load_city_data_in_chunks(city):
    total_buildings += len(chunk)
    total_solar_potential += chunk['annual_solar_potential_kwh'].sum()
    total_estimated_savings += chunk['annual_estimated_savings_usd'].sum()

st.sidebar.write(f"Total buildings: {total_buildings:,}")
st.sidebar.write(f"Total solar potential: {total_solar_potential:,.2f} kWh")
st.sidebar.write(f"Total estimated savings: ${total_estimated_savings:,.2f}")