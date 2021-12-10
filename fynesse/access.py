from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import mongodb
import sqlite"""

import pymysql
import math
import osmnx as ox

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError



# --------------------------------------------------------------------------------------------
# SETTING UP THE DATABASE
# --------------------------------------------------------------------------------------------

#  provided earlier in the course
def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database
                               )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn

def get_credentials_from_file():
    database_details = {"url": "database-arw92.cgrre17yxw11.eu-west-2.rds.amazonaws.com", 
                    "port": 3306}
    with open("credentials.yaml") as file:
        credentials = yaml.safe_load(file)
    username = credentials["username"]
    password = credentials["password"]
    url = database_details["url"]
    return username, password, url

def uploaddb_csv(table, filename):
  username, password, url = get_credentials_from_file()
  conn = create_connection(username, password, url, "arw92-database")
  load = f"""LOAD DATA LOCAL INFILE %s INTO TABLE `{table}` 
              FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
              LINES STARTING BY '' TERMINATED BY '\n';"""
  with conn:
    with conn.cursor() as cursor:
      cursor.execute(load, filename)
    conn.commit()


def upload_pp_data():
    username, password, url = get_credentials_from_file()
    for y in range(1995,2022):
        for part in ["-part1", "-part2"]:
            filename = "pp-"+str(y)+part+".csv"
            uploaddb_csv(username, password, url, "pp_data", filename)

def upload_pc_data():
    username, password, url = get_credentials_from_file()
    uploaddb_csv(username, password, url, "postcode_data", "open_postcode_geo.csv")







# --------------------------------------------------------------------------------------------
# QUERYING THE DATABASE
# --------------------------------------------------------------------------------------------

def select_top(conn, table,  n):
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM `{table}` LIMIT {n}')

    rows = cur.fetchall()
    return rows

def test_conn(username, password, url, database_name):
    conn = create_connection(username, password, url, database_name)
    rows = select_top(conn, "pp_data", 1)
    for x in rows:
        print(x)




# --------------------------------------------------------------------------------------------
# JOINING TABLES ON THE FLY
# --------------------------------------------------------------------------------------------

def get_all_properties_bounding_box(lat,lon,dist):
  """
  Given a latitude and longitude, draws a bounding box of size distance around 
  that point, and collects all property records within that bounding box.

  This is done by joining the two tables together on postcode.
  """
  # Calculates the latitude and longitude for this distance in 
  # each direction from the point
  up = (dist/11.1)*0.1
  across = (dist*0.1)/(11.1*math.cos(math.radians(lat)))

  north = lat + up
  south = lat - up
  east = lon + across
  west = lon - across

  username, password, url = get_credentials_from_file()
  conn = create_connection(username, password, url, "arw92-database")

  cur = conn.cursor()
  cur.execute(f"""
  SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type,	pp.town_city as town_city, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector
  FROM (SELECT * FROM `pp_data`) pp
  INNER JOIN (SELECT 
              postcode, postcode_sector, lattitude, longitude 
              FROM `postcode_data`
              WHERE lattitude <= {north} AND lattitude >= {south} AND longitude <= {east} AND longitude >= {west}) pc
  ON pp.postcode = pc.postcode""")

  cols= [column[0] for column in cur.description] 
  rows = cur.fetchall()

  return rows, cols, north, south, east, west

def get_properties_bounding_box(lat,lon,dist,pt,date):
  """
  Given a latitude and longitude, draws a bounding box of size distance around 
  that point, and collects all property records of type pt within that bounding 
  box, from the 5 years before that date.

  This is done by joining the two tables together on postcode.
  """
  # Calculates the latitude and longitude for this distance in 
  # each direction from the point
  up = (dist/11.1)*0.1
  across = (dist*0.1)/(11.1*math.cos(math.radians(lat)))

  north = lat + up
  south = lat - up
  east = lon + across
  west = lon - across

  username, password, url = get_credentials_from_file()
  conn = create_connection(username, password, url, "arw92-database")

  cur = conn.cursor()
  cur.execute(f"""
  SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type,	pp.town_city as town_city, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector
  FROM (SELECT * FROM `pp_data` WHERE property_type = '{pt}' AND DATEDIFF('{date}', date_of_transfer) < 365*10 AND date_of_transfer < '{date}') pp
  INNER JOIN (SELECT 
              postcode, postcode_sector, lattitude, longitude 
              FROM `postcode_data`
              WHERE lattitude <= {north} AND lattitude >= {south} AND longitude <= {east} AND longitude >= {west}) pc
  ON pp.postcode = pc.postcode""")

  cols= [column[0] for column in cur.description] 
  rows = cur.fetchall()

  return rows, cols, north, south, east, west


def get_all_properties_within_dist(lat, lon, dist):
  query = f"""SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type,	pp.town_city as town_city, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector
  FROM (SELECT * FROM `pp_data`) pp
  INNER JOIN (SELECT 
              postcode, lattitude, longitude,
              (6371 *
                acos(cos(radians({lat})) * 
                cos(radians(lattitude)) * 
                cos(radians(longitude) - 
                radians({lon})) + 
                sin(radians({lat})) * 
                sin(radians(lattitude)))
              ) AS distance 
              FROM `postcode_data` 
              HAVING distance < {dist}) pc
  ON pp.postcode = pc.postcode;"""

  username, password, url = get_credentials_from_file()
  conn = create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(query)
  cols= [column[0] for column in cur.description]
  rows = cur.fetchall()
  return rows, cols

def get_properties_within_dist_type_date(lat, lon, dist, pt, date):
  query = f"""SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type,	pp.town_city as town_city, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector
  FROM (SELECT * FROM `pp_data` WHERE property_type = '{pt}' AND DATEDIFF('{date}', date_of_transfer) < 365*10 AND date_of_transfer < '{date}') pp
  INNER JOIN (SELECT 
              postcode, lattitude, longitude,
              (6371 *
                acos(cos(radians({lat})) * 
                cos(radians(lattitude)) * 
                cos(radians(longitude) - 
                radians({lon})) + 
                sin(radians({lat})) * 
                sin(radians(lattitude)))
              ) AS distance 
              FROM `postcode_data` 
              HAVING distance < {dist}) pc
  ON pp.postcode = pc.postcode;"""

  username, password, url = get_credentials_from_file()
  conn = create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(query)
  cols= [column[0] for column in cur.description]
  rows = cur.fetchall()
  return rows, cols

# --------------------------------------------------------------------------------------------
# GETTING POINTS OF INTEREST
# --------------------------------------------------------------------------------------------



def get_tags(kind):
  tags = {"amenity":True}
  if kind == "schools":
    tags = {
      "amenity": ["high_school","kindergarten","primary_school","prep_school","school","nursery","preschool"]
    }
  elif kind == "higher_education":
    tags = {
     "amenity": ["college","university"]   
    }
  elif kind == "transport":
    tags = {
      "amenity":["ferry_terminal","taxi","bicycle_parking","bicycle_repair_station","charging_station","fuel","grit_bin","parking","parking_space"],
      "waterway":["canal"]
    }
  elif kind == "public_transport":
    tags = {
      "highway":["bus_stop"],
      "railway":["station", "tram_stop"]
    }
  elif kind == "exercise":
    tags = {
      "leisure":["pitch","fitness_centre","sports_centre"],
      "amenity": ["training""stables","swimming_pool"]
    }
  elif kind == "parks_and_rec":
    tags = {
      "leisure":["paddling_pool","park","playground","common",],
      "amenity": ["park", "social_centre", "youth_centre"],
      "landuse":["recreation_ground","village_green","flowerbed"]
    }
  elif kind == "services":
    tags = {
      "amenity":["bank", "grave_yard", "shop","marketplace"],
      "shop":["supermarket","convenience","variety_store","general","butcher","bakery","beverages","farm","frozen_food","greengrocer","kiosk","clothes","charity","second_hand","chemist","hairdresser","optician","computer","bicycle","books","newsagent"],
      "landuse":["retail","garages"],
      "craft":["carpenter","electrician"],
      "office":["estate_agent","charity"]
    }
  elif kind == "public_services":
    tags = {
      "amenity":["letter_box", "fire_station", "hospital", "library", "place_of_worship","police","polling_station","post_box","post_depot","post_office","social_centre", "youth_centre","atm","dentist","doctors","pharmacy","townhall"],
      "landuse":["hospital","religious"]
    }
  elif kind == "commercial":
    tags = {
        "landuse":["commercial","office"],
        "office":["insurance","lawyer","accountant"]
    }
  elif kind == "leisure":
    tags = {
      "leisure":["outdoor_seating","picnic","beach","beach_resort"],
      "amenity":["arts_centre","concert_hall","theatre","cafe","bar","fast_food","restaurant","pub","cinema","events_venue","fountain","nightclub"],
      "landuse":["reservoir"]
    }
  elif kind == "rural":
    tags = {
        "landuse":["grazing", "farmyard","agriculture","allotments","animal_keeping","conservation","farmland","farm","field","forestry","greenery","livestock","meadow","meadow_orchard","pond","trees","wood","grass"],
        "crop":["grass","hay","cereal","barley","corn","oats","rye","wheat","potato","sugar_beet","sunflower","rape","vegetable","berries","hop","flowers","lavender","fast_growing_wood","timber","poultry","dairy"],
        "amenity":["nature_reserve"],
        "tourism":["camp_site","wilderness_hut", "picnic_site","viewpoint"],
        "natural":["peak","water", "spring","wood","grassland","fell","tundra","heath","moor","scrub"],
        "waterway":["river", "stream", "canal", "lock_gate"]
    }
  elif kind == "industrial":
    tags = {
        "landuse":["mine","quarry","reservoir","wholesale","port","industrial","harbour","depot","landfill"],
        "amenity":["waste_dump_site","waste_transfer_station","waste_disposal","prison"],
        "man_made":["wastewater_plant"],
        "power":["plant"], 
    }
  return tags

def features_around_point_bbox(lat, lon, d, kind):
  """Given a point and a distance, returns all pois of type kind in a bounding box of that distance around the point."""
  return ox.geometries.geometries_from_point((lat,lon), get_tags(kind), dist=d*1000)

def features_around_point_wbounds(n, s, e, w, kind):
  """Given a set of bounds, returns all pois of type kind in that bounding box."""
  return ox.geometries_from_bbox(n, s, e, w, get_tags(kind))

