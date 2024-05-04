import random
import datetime
import psycopg2
from shapely.geometry import Point, LineString
import geopandas as gpd

# Define the start and finish points
start_point = Point(36.26572, 32.59500)
finish_point = Point(36.34925, 32.44838)

# Connect to PostgreSQL/PostGIS
conn = psycopg2.connect("dbname='postgres' user='kahler' host='localhost' password='3755'")
cur = conn.cursor()

# Create a table to store the biker paths
cur.execute("""
    CREATE TABLE biker_paths (
        id SERIAL PRIMARY KEY,
        biker_id INT,
        timestamp TIMESTAMP,
        geom GEOMETRY(LINESTRING, 4326)
    )
""")

# Generate random biker paths and insert into the table
biker_count = 10
total_time = datetime.timedelta(minutes=20)
time_interval = datetime.timedelta(seconds=15)

for biker_id in range(1, biker_count + 1):
    current_time = datetime.datetime.now()
    current_point = start_point
    biker_path = [current_point]

    while current_time < datetime.datetime.now() + total_time:
        # Generate random next point within bounding box
        next_point = Point(
            random.uniform(current_point.x - 0.01, current_point.x + 0.01),
            random.uniform(current_point.y - 0.01, current_point.y + 0.01)
        )
        biker_path.append(next_point)

        # Insert point into the table
        cur.execute("""
            INSERT INTO biker_paths (biker_id, timestamp, geom)
            VALUES (%s, %s, ST_SetSRID(ST_MakeLine(ARRAY[%s, %s]), 4326))
        """, (biker_id, current_time, current_point.coords[0], next_point.coords[0]))

        current_point = next_point
        current_time += time_interval

    # Add the finish point
    biker_path.append(finish_point)

    # Insert the complete path into the table
    cur.execute("""
        INSERT INTO biker_paths (biker_id, timestamp, geom)
        VALUES (%s, %s, ST_SetSRID(ST_MakeLine(%s), 4326))
    """, (biker_id, current_time, [point.coords[0] for point in biker_path]))

# Commit the changes and close the connection
conn.commit()
conn.close()
