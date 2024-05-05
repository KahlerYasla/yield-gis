import psycopg2
from shapely.geometry import MultiPoint, Polygon
from shapely.wkt import dumps

# Connect to the database
conn = psycopg2.connect("dbname='postgres' user='kahler' host='localhost' password='3755'")
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
    cur.execute("SELECT point_every_time_interval FROM time_nodes WHERE time_in_seconds = %s", (sec,))
    points = cur.fetchall()

    # Create a MultiPoint geometry from fetched points
    multi_point = MultiPoint([point[0] for point in points])

    # Create a convex hull polygon from MultiPoint
    convex_hull_polygon = multi_point.convex_hull

    # Insert the convex hull polygon into time_polygons table
    cur.execute("INSERT INTO time_polygons (time_in_seconds, polygon) VALUES (%s, ST_GeomFromText(%s, 4326))", (sec, dumps(convex_hull_polygon)))

# Commit changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()
