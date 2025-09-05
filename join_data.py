# importing necessary libs
import pandas as pd
import geopandas as gpd

# Step 1: Load the cleaned data
lga_sen_path = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\cleaned_LGA_SEN.csv"
lga_sen = pd.read_csv(lga_sen_path)

print("LGA-SEN data loaded. First 5 rows:")
print(lga_sen.head())

print(f"\nTotal rows: {len(lga_sen)}")

# Step 2: Load the LGA boundary shapefile
shapefile_path = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\datasets\admin_bndry.shp"
lga_map = gpd.read_file(shapefile_path)

print("\nLGA map data loaded. First 5 rows:")
print(lga_map.head())
print(f"\nMap CRS (coordinate system): {lga_map.crs}")
print(f"Number of LGAs: {len(lga_map)}")


# Step 3: Load health facility locations and convert to GeoDataFrame
hf_csv_path = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\datasets\hf_locations.csv"
hf_locations = pd.read_csv(hf_csv_path, encoding='latin1')

# Convert to GeoDataFrame using latitude and longitude
gdf_hf = gpd.GeoDataFrame(
    hf_locations,
    geometry=gpd.points_from_xy(hf_locations['longitude'], hf_locations['latitude']),
    crs='EPSG:4326'  # Standard geographic coordinate system
)

print("\nHealth facilities converted to map points.")
print("First 5 rows with geometry:")
print(gdf_hf.head())

print(f"Total facilities: {len(gdf_hf)}")

# Step 4: Load health facility scoring data
scoring_path = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\datasets\hf_personnel_scoring.tab"
gdf_score = gpd.read_file(scoring_path)

print("\nScoring data loaded. First 5 rows:")
print(gdf_score.head())
print(f"Scoring data columns: {list(gdf_score.columns)}")
print(f"Number of scored facilities: {len(gdf_score)}")

# Step 5: Merge facility data with scores
gdf_hf_scored = gdf_hf.merge(
    gdf_score,
    left_on='globalid',    # From gdf_hf
    right_on='hf_uuid',    # From gdf_score
    how='left'             # Keep all facilities
)

print("\nFacility data merged with scores. First 5 rows:")
print(gdf_hf_scored[['hf_name', 'latitude', 'longitude', 'globalid', 'hf_total_score']].head())

# Check for missing scores
missing = gdf_hf_scored['hf_total_score'].isna().sum()
print(f"Facilities with missing score: {missing}")

# Step 6: Clean the score column
gdf_hf_scored['hf_total_score'] = pd.to_numeric(gdf_hf_scored['hf_total_score'], errors='coerce')

print("\nScore column cleaned and converted to numeric.")
print("First 5 rows after cleaning:")
print(gdf_hf_scored[['hf_name', 'globalid', 'hf_total_score']].head())

print(f"Facilities with valid numeric scores: {gdf_hf_scored['hf_total_score'].count()}")

# Step 7: Use the senatorial_district already in the facility data
print("\nUsing 'senatorial_district' from hf_locations.csv")
print("Unique senatorial districts:", gdf_hf_scored['senatorial_district'].nunique())
print("List of districts:")
print(gdf_hf_scored['senatorial_district'].unique())

# Step 8: Clean senatorial_district names
gdf_hf_scored['senatorial_district_clean'] = (
    gdf_hf_scored['senatorial_district']
    .str.strip()                    # Remove leading/trailing spaces
    .str.replace('\x96', '-', regex=False)  # Replace weird dash with normal dash
    .str.replace('  ', ' ', regex=False)    # Fix double spaces
)

print("\nCleaned senatorial districts (after cleaning):")
print(gdf_hf_scored['senatorial_district_clean'].unique())
print(f"Total unique cleaned districts: {gdf_hf_scored['senatorial_district_clean'].nunique()}")

# Step 9: Find the best-ranked facility in each senatorial district
best_per_sen = gdf_hf_scored[
    gdf_hf_scored['hf_total_score'] == gdf_hf_scored.groupby('senatorial_district_clean')['hf_total_score'].transform('max')
].drop_duplicates(subset=['senatorial_district_clean'])

# Keep only one row if there's a tie (in case two have same max score)
best_per_sen = best_per_sen.drop_duplicates(subset=['senatorial_district_clean'])

print("\n✅ Best-ranked facility per senatorial district:")
print(f"Total districts covered: {len(best_per_sen)}")
print("\nTop 5 highest scores:")
print(best_per_sen[['senatorial_district_clean', 'hf_name', 'hf_total_score']].sort_values('hf_total_score', ascending=False).head())

# Step 10: Save processed data to files
output_folder = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\processed"

# Create folder if it doesn't exist
import os
os.makedirs(output_folder, exist_ok=True)

# Save best facility per district
best_per_sen.to_file(os.path.join(output_folder, "best_facility_per_sen.geojson"), driver="GeoJSON")

# Save all scored facilities
gdf_hf_scored.to_file(os.path.join(output_folder, "all_facilities_scored.geojson"), driver="GeoJSON")

print(f"\n💾 Files saved to {output_folder}:")
print("  - best_facility_per_sen.geojson")
print("  - all_facilities_scored.geojson")

# Step 11: Export to SQLite
import sqlite3
import os

sqlite_db_path = r"C:\Users\USER\Desktop\eHealthOrg\Assignment 1\nigeria_health_facilities.sqlite"
os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)

# Convert geometry to WKT (text) for SQLite storage
best_per_sen_sql = best_per_sen.copy()
best_per_sen_sql['geometry'] = best_per_sen_sql.geometry.to_wkt()

gdf_hf_scored_sql = gdf_hf_scored.copy()
gdf_hf_scored_sql['geometry'] = gdf_hf_scored_sql.geometry.to_wkt()

# Save to SQLite
with sqlite3.connect(sqlite_db_path) as conn:
    best_per_sen_sql.to_sql("best_facility_per_sen", conn, if_exists="replace", index=False)
    gdf_hf_scored_sql.to_sql("all_facilities_scored", conn, if_exists="replace", index=False)

print(f"\n✅ Final data saved to SQLite: {sqlite_db_path}")