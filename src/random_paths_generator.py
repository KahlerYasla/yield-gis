import math
import random
import psycopg2
from shapely.geometry import Point, LineString

# Connect to PostgreSQL/PostGIS
conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='1234'")
cur = conn.cursor()

# Create a table to store the biker paths
def init_db_tables(cur):
    cur.execute("""
        DROP TABLE IF EXISTS time_nodes CASCADE;
        DROP TABLE IF EXISTS biker_paths CASCADE;
        CREATE TABLE biker_paths (
            id SERIAL PRIMARY KEY,
            biker_id INT,
            point_every_time_interval GEOMETRY(LINESTRING, 4326)
        );

        CREATE TABLE time_nodes (
            id SERIAL PRIMARY KEY,
            biker_id INT,
            time_in_seconds INT,
            caloric_burn_until_now INT,
            distance_until_now INT,
            point_every_time_interval GEOMETRY(POINT, 4326),
            FOREIGN KEY (biker_id) REFERENCES biker_paths(id)
        );
    """)

# Define the start and finish points
start_point = Point(32.59500, 36.26572)
finish_point = Point(32.44838, 36.34925)

# Randomly generate the step_distance_coefficient (propotional to velocity of the biker)
step_distance_coefficient = random.uniform(0.001, 0.003)

# Generate random next point within bounding box and faced max 30 degrees from the finish point
def generate_next_point(current_point):
    
    # Get the degree of the line between the current point and the finish point
    angle = random.uniform(-45, 45)
    angle = math.radians(angle)
    angle = math.atan2(finish_point.y - current_point.y, finish_point.x - current_point.x) + angle

    # Calculate the coordinates of the next point
    x = current_point.x + step_distance_coefficient * math.cos(angle)
    y = current_point.y + step_distance_coefficient * math.sin(angle)

    # Create the next point
    next_point = Point(x, y)

    return next_point

def get_ascent_descent(cur):
    for i in range(1, 11):
        qstr = """
            with data_points as (
            select l.time_in_seconds, ST_Value(e.rast, ST_Transform(l.point_every_time_interval, 4326)) as elevation, 
                RANK () OVER (ORDER BY time_in_seconds) as ordinal
            from time_nodes l
            inner join n36_e032_1arc_v3 e ON ST_Intersects(ST_ConvexHull(e.rast), ST_Transform(l.point_every_time_interval, 4326)) where l.biker_id = {}
        ), elevation_deltas as (
            select dp1.ordinal, 
                dp2.elevation - dp1.elevation as delta, 
                case when dp1.elevation < dp2.elevation then 'ascent' else 'descent' end as direction
            from data_points dp1
            inner join data_points dp2 on dp2.ordinal = dp1.ordinal + 1
        )
        select (select Count(delta) from elevation_deltas where direction = 'ascent') as totalAscent,
        (select Count(delta) from elevation_deltas where direction = 'descent') as totalDescent
        """.format(i)
        cur.execute(qstr)
        result = cur.fetchone()
        print("[Biker {}] Total ascent: {}, total descent: {}".format(i, result[0], result[1]))
    print()

def get_scoreboard(cur):
    cur.execute("select biker_id, time_in_seconds from time_nodes where ST_Setsrid(ST_Point(32.44838, 36.34925), 4326) = point_every_time_interval order by time_in_seconds asc;")
    result = cur.fetchall()
    print("WINNER: Biker", result[0][0])
    print("Placement Order:")
    for i in range(0, len(result)):
        print("Biker {} with {}:{}:00.000".format(str(result[i][0]).ljust(2), str(result[i][1]//3600).rjust(2, "0"), str(int((result[i][1] / 60) % 60)).rjust(2, "0")))
    print()

def get_kilometers_and_calories(cur):
    cur.execute("select biker_id, caloric_burn_until_now, distance_until_now from time_nodes where ST_Setsrid(ST_Point(32.44838, 36.34925), 4326) = point_every_time_interval;")
    result = cur.fetchall()
    for i in range(10):
        print("[Biker {}] {} KCal, {} KM".format(result[i][0], result[i][1], result[i][2]/1000))
    print()

def generate_data_into_db(cur):
    # Generate random biker paths and insert into the table
    biker_count = 10
    total_time = 20*60
    time_interval = 60

    for biker_id in range(1, biker_count + 1):
        current_time = 0
        current_point = start_point
        caloric_burn_until_now = 0.0
        distance_until_now = 0.0

        step_distance_coefficient = random.uniform(0.001, 0.005)
        while current_point.distance(finish_point) > step_distance_coefficient:
            # Generate random next point within bounding box
            next_point = generate_next_point(current_point)
            # Insert lines into the table
            cur.execute("""
                INSERT INTO biker_paths (biker_id, point_every_time_interval)
                VALUES (%s, ST_SetSRID(ST_MakeLine(ST_Point(%s,%s), ST_Point(%s,%s)), 4326))
            """, (biker_id, current_point.x, current_point.y, next_point.x, next_point.y))

            # Insert the time node into the table
            cur.execute("""
                INSERT INTO time_nodes (time_in_seconds, biker_id, caloric_burn_until_now, distance_until_now, point_every_time_interval)
                VALUES (%s, %s, %s, %s, ST_Point(%s,%s))
            """, (current_time, biker_id, caloric_burn_until_now, distance_until_now, current_point.x, current_point.y))

            distance_between_points = current_point.distance(next_point) * 111000
            distance_until_now += distance_between_points
            caloric_burn_until_now += 27 * distance_between_points/1000

            current_point = next_point
            current_time += time_interval

        # Insert the lats line into the table
        cur.execute("""
            INSERT INTO biker_paths (biker_id, point_every_time_interval)
            VALUES (%s, ST_SetSRID(ST_MakeLine(ST_Point(%s,%s), ST_Point(%s,%s)), 4326))
        """, (biker_id, current_point.x, current_point.y, finish_point.x, finish_point.y))

        cur.execute("""
            INSERT INTO time_nodes (time_in_seconds, biker_id, caloric_burn_until_now, distance_until_now, point_every_time_interval)
            VALUES (%s, %s, %s, %s, ST_Point(%s,%s))
        """, (current_time, biker_id, caloric_burn_until_now, distance_until_now, current_point.x, current_point.y))

        distance_between_points = current_point.distance(finish_point) * 111000
        distance_until_now += distance_between_points
        caloric_burn_until_now += 27 * distance_between_points/1000
        current_time += time_interval

        cur.execute("""
            INSERT INTO time_nodes (time_in_seconds, biker_id, caloric_burn_until_now, distance_until_now, point_every_time_interval)
            VALUES (%s, %s, %s, %s, ST_Point(%s,%s))
        """, (current_time, biker_id, caloric_burn_until_now, distance_until_now, finish_point.x, finish_point.y))

#init_db_tables(cur)
#generate_data_into_db(cur)
get_ascent_descent(cur)
get_scoreboard(cur)
get_kilometers_and_calories(cur)
# Commit the changes and close the connection
cur.close()
conn.commit()
conn.close()