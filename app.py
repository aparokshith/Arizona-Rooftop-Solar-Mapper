import streamlit as st
import folium
from streamlit_folium import folium_static
import geopandas as gpd
import json

# Set page configuration
st.set_page_config(page_title="Arizona Rooftop Solar Potential Mapper", layout="wide")

# Title
st.title("Arizona Rooftop Solar Potential Mapper")

# Sidebar for city selection
city = st.sidebar.selectbox(
    "Choose a city",
    ["Phoenix", "Tucson", "Tempe", "Mesa", "Flagstaff"]
)

@st.cache_data
def load_data(city):
    gdf = gpd.read_file(f"data/processed/{city.lower()}_solar_potential.geojson")
    return gdf

# Load data
data = load_data(city)

# Create map
m = folium.Map(location=[data.geometry.centroid.y.mean(), data.geometry.centroid.x.mean()], zoom_start=12)

# Function to style the GeoJSON features
def style_function(feature):
    solar_potential = feature['properties']['annual_solar_potential_kwh']
    if solar_potential > 10000:
        color = '#ff0000'  # Red for high potential
    elif solar_potential > 5000:
        color = '#ffff00'  # Yellow for medium potential
    else:
        color = '#00ff00'  # Green for low potential
    return {
        'fillColor': color,
        'color': 'black',
        'weight': 1,
        'fillOpacity': 0.7
    }

# Add GeoJSON to map
folium.GeoJson(
    data,
    style_function=style_function,
    tooltip=folium.GeoJsonTooltip(
        fields=['building_id', 'roof_area_sqm', 'annual_solar_potential_kwh', 'annual_estimated_savings_usd'],
        aliases=['Building ID', 'Roof Area (sq m)', 'Annual Solar Potential (kWh)', 'Annual Estimated Savings ($)'],
        style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
    )
).add_to(m)

# Display map
st_data = folium_static(m, width=1200, height=600)

# Display statistics
st.subheader("City Statistics")
st.write(f"Total Buildings: {len(data)}")
st.write(f"Average Solar Potential: {data['annual_solar_potential_kwh'].mean():.2f} kWh")
st.write(f"Total Estimated Savings: ${data['annual_estimated_savings_usd'].sum():.2f}")

# Optional: Add a chart
import plotly.express as px

fig = px.histogram(data, x="annual_solar_potential_kwh", nbins=50,
                   title=f"Distribution of Annual Solar Potential in {city}")
st.plotly_chart(fig)

# Add information about the project
st.sidebar.info(
    "This app visualizes the solar potential for rooftops in major Arizona cities. "
    "The data is based on building footprints, solar radiation data, and "
    "simplified solar potential calculations."
)

# Add a disclaimer
st.sidebar.warning(
    "Disclaimer: This tool provides estimates based on simplified calculations. "
    "For accurate assessments, please consult with a professional solar installer."
)