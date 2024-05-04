import math
import random
import datetime
import psycopg2
from shapely.geometry import Point, LineString

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

# Define the start and finish points
start_point = Point(32.59500, 36.26572)
finish_point = Point(32.44838, 36.34925)

def generate_next_point(current_point):
    # Generate random next point within bounding box and faced max 30 degrees from the finish point
    
    # Get the degree of the line between the current point and the finish point
    angle = random.uniform(-30, 30)
    angle = math.radians(angle)
    angle = math.atan2(finish_point.y - current_point.y, finish_point.x - current_point.x) + angle

    # Calculate the coordinates of the next point
    x = current_point.x + 0.0025 * math.cos(angle)
    y = current_point.y + 0.01 * math.sin(angle)

    # Create the next point
    next_point = Point(x, y)

    return next_point

# Generate random biker paths and insert into the table
biker_count = 10
total_time = datetime.timedelta(minutes=20)
time_interval = datetime.timedelta(seconds=20)

for biker_id in range(1, biker_count + 1):
    current_time = datetime.datetime.now()
    current_point = start_point

    while current_time < datetime.datetime.now() + total_time:
        # Generate random next point within bounding box
        next_point = generate_next_point(current_point)

        # Insert point into the table
        cur.execute("""
            INSERT INTO biker_paths (biker_id, timestamp, geom)
            VALUES (%s, %s, ST_SetSRID(ST_MakeLine(ST_Point(%s,%s), ST_Point(%s,%s)), 4326))
        """, (biker_id, current_time, current_point.x, current_point.y, next_point.x, next_point.y))

        current_point = next_point
        current_time += time_interval

    # Insert the complete path into the table
    cur.execute("""
        INSERT INTO biker_paths (biker_id, timestamp, geom)
        VALUES (%s, %s, ST_SetSRID(ST_MakeLine(ST_Point(%s,%s), ST_Point(%s,%s)), 4326))
    """, (biker_id, current_time, current_point.x, current_point.y, finish_point.x, finish_point.y))

# Commit the changes and close the connection
conn.commit()
conn.close()

