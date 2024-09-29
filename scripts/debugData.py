import geopandas as gpd
import pandas as pd
import os
from tabulate import tabulate
os.chdir('/')

def load_and_analyze_city_data(city):
    file_path = f"data/processed/{city.lower()}_solar_potential.geojson"
    print(file_path)
    if not os.path.exists(file_path):
        print(f"No data file found for {city}")
        return None

    gdf = gpd.read_file(file_path)

    stats = {
        "City": city,
        "Total Buildings": len(gdf),
        "Avg Solar Potential (kWh)": gdf['annual_solar_potential_kwh'].mean(),
        "Total Solar Potential (MWh)": gdf['annual_solar_potential_kwh'].sum() / 1000,  # Convert to MWh
        "Avg Estimated Savings ($)": gdf['annual_estimated_savings_usd'].mean(),
        "Total Estimated Savings ($M)": gdf['annual_estimated_savings_usd'].sum() / 1_000_000,  # Convert to millions
        "Max Estimated Savings ($)": gdf['annual_estimated_savings_usd'].max(),
        "Min Estimated Savings ($)": gdf['annual_estimated_savings_usd'].min(),
        "Avg Roof Area (sq m)": gdf['roof_area_sqm'].mean(),
    }

    return stats


def main():
    cities = ['phoenix', 'tucson', 'tempe', 'mesa', 'flagstaff']
    all_stats = []

    for city in cities:
        city_stats = load_and_analyze_city_data(city)
        if city_stats:
            all_stats.append(city_stats)

    if all_stats:
        df = pd.DataFrame(all_stats)
        df = df.set_index('City')

        # Round numeric columns
        numeric_columns = df.select_dtypes(include=['float64']).columns
        df[numeric_columns] = df[numeric_columns].round(2)

        print("\nCity Statistics:")
        print(tabulate(df, headers='keys', tablefmt='pretty'))

        # Additional Analysis
        print("\nAdditional Analysis:")
        print(f"Total buildings across all cities: {df['Total Buildings'].sum():,}")
        print(f"Total solar potential across all cities: {df['Total Solar Potential (MWh)'].sum():,.2f} MWh")
        print(f"Total estimated savings across all cities: ${df['Total Estimated Savings ($M)'].sum():,.2f}M")

        # City Rankings
        print("\nCity Rankings:")
        for column in ['Avg Solar Potential (kWh)', 'Avg Estimated Savings ($)', 'Total Solar Potential (MWh)']:
            print(f"\nTop 3 cities by {column}:")
            print(df.sort_values(by=column, ascending=False)[column].head(3))
    else:
        print("No data available for any city.")


if __name__ == "__main__":
    main()