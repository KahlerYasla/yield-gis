import numpy as np
from shapely.geometry import LineString, Point
import psycopg2
from datetime import datetime, timedelta

# Define start and end points
start_point = Point(36.26572, 32.59500)
end_point = Point(36.34925, 32.44838)

# Connect to PostGIS database
conn = psycopg2.connect("dbname='postgres' user='kahler' host='localhost' password='3755'")
cur = conn.cursor()

# Function to generate random path
def generate_random_path(start_point, end_point, duration):
    # Generate random points between start and end points
    num_points = int(duration.total_seconds() / 60)  # Number of points per minute
    lons = np.linspace(start_point.x, end_point.x, num_points)
    lats = np.linspace(start_point.y, end_point.y, num_points)
    
    # Create LineString from the random points
    path = LineString(zip(lons, lats))
    return path

# Generate and store random paths for 10 racers
for racer_id in range(1, 11):
    # Generate random path for each racer
    random_path = generate_random_path(start_point, end_point, timedelta(minutes=20))
    
    # Insert path into PostGIS table
    cur.execute("INSERT INTO racer_paths (racer_id, path_geom) VALUES (%s, ST_GeomFromText(%s, 4326))", (racer_id, random_path.wkt))

# Commit changes and close connection
conn.commit()
conn.close()
