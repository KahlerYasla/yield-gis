import psycopg2
from shapely.geometry import MultiPoint, Polygon, Point

# Connect to the database
conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='1234'")
cur = conn.cursor()

# Create a table to store polygons
cur.execute("""
    DROP TABLE IF EXISTS time_polygons;
    CREATE TABLE IF NOT EXISTS time_polygons (
        id SERIAL PRIMARY KEY,
        time_in_seconds INT,
        polygon GEOMETRY(POLYGON, 4326)
    );
""")

# Find maximum time_in_seconds
cur.execute("SELECT MAX(time_in_seconds) FROM time_nodes")
max_time = cur.fetchone()[0]

# Iterate through all seconds
for sec in range(1, max_time + 1):
    # Fetch points at current second
    cur.execute("SELECT ST_AsText(point_every_time_interval) FROM time_nodes WHERE time_in_seconds = %s", (sec,))
    points = cur.fetchall()

    if len(points) < 3:
        continue

    print("Processing second:", sec, "//", len(points), "points")

    cur.execute("SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('MULTIPOINT(" + ", ".join([point[0][6:-1] for point in points]) + ")')))")
    polygon = cur.fetchone()[0]

    if "LINESTRING" in polygon or "POINT" in polygon:
        continue

    # Insert the convex hull polygon into time_polygons table
    cur.execute("INSERT INTO time_polygons (time_in_seconds, polygon) VALUES (%s, %s)", (sec, polygon))

# Commit changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()
