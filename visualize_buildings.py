import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import folium_static
# Set page configuration
st.set_page_config(page_title="Arizona Building Footprints Viewer", layout="wide")


@st.cache_data
def load_city_boundaries():
    return gpd.read_file('data/raw/city_boundaries/tl_2023_04_place.shp')


@st.cache_data
def load_buildings(city_name, city_boundary):
    all_buildings = gpd.read_file('data/raw/building_footprints/Arizona.geojson')
    all_buildings = all_buildings.to_crs(city_boundary.crs)
    city_buildings = gpd.sjoin(all_buildings, city_boundary, predicate='intersects')
    return city_buildings


# Load city boundaries
cities = load_city_boundaries()

# Sidebar for city selection
st.sidebar.title("City Selection")
city_name = st.sidebar.selectbox("Choose a city", cities['NAME'].sort_values())

# Main content
st.title(f"Building Footprints in {city_name}")

if city_name:
    # Get the boundary for the selected city
    city_boundary = cities[cities['NAME'] == city_name]

    if not city_boundary.empty:
        # Load buildings for the selected city
        with st.spinner(f"Loading buildings for {city_name}..."):
            city_buildings = load_buildings(city_name, city_boundary)

        st.write(f"Total buildings found: {len(city_buildings)}")

        # Create a folium map
        m = folium.Map(location=city_boundary.geometry.centroid.iloc[0].coords[0][::-1], zoom_start=12)

        # Add city boundary to the map
        folium.GeoJson(city_boundary.geometry).add_to(m)

        # Add buildings to the map
        folium.GeoJson(
            city_buildings.geometry,
            style_function=lambda feature: {
                'fillColor': 'red',
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.5,
            }
        ).add_to(m)

        # Display the map
        folium_static(m)

        # Display sample data
        st.subheader("Sample Building Data")
        st.write(city_buildings.head())

    else:
        st.error(f"No boundary data found for {city_name}")
else:
    st.info("Please select a city from the sidebar to view building footprints.")

# Add some information about the data
st.sidebar.info(
    "This application visualizes building footprints for cities in Arizona. "
    "The data is sourced from Microsoft Building Footprints dataset and "
    "city boundaries from U.S. Census Bureau TIGER/Line Shapefiles."
)