from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import mongodb
import sqlite"""

import pymysql

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError



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


def uploaddb_csv(filename):
  conn = create_connection(username, password, url, "arw92-database")
  load = """LOAD DATA LOCAL INFILE %s INTO TABLE `pp_data` 
              FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
              LINES STARTING BY '' TERMINATED BY '\n';"""
  with conn:
    with conn.cursor() as cursor:
      cursor.execute(load, filename)
    conn.commit()


def upload_pp_data():
    for y in range(1995,2022):
        for part in ["-part1", "-part2"]:
            filename = "pp-"+str(y)+part+".csv"
            uploaddb_csv(filename)

def upload_pc_data():
    uploaddb_csv("open_postcode_geo.csv")



































# 
# CONNECTING TO THE DATABASE
# 

def select_top(conn, table,  n):
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM `{table}` LIMIT {n}')
    # cur.execute(f'SELECT * FROM `{table}`;')

    rows = cur.fetchall()
    return rows

def test_conn(database_name):
    conn = create_connection(username, password, url, database_name)
    rows = select_top(conn, "pp_data", 1)
    for x in rows:
        print(x)




# 
# Joining tables
# 
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

  conn = create_connection(username, password, url, "arw92-database")

  cur = conn.cursor()
  cur.execute(f"""
  SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type,	pp.town_city as town_city, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector
  FROM (SELECT * FROM `pp_data` WHERE property_type = '{pt}' AND DATEDIFF('{date}', date_of_transfer) < 365*5 AND date_of_transfer < '{date}') pp
  INNER JOIN (SELECT 
              postcode, postcode_sector, lattitude, longitude 
              FROM `postcode_data`
              WHERE lattitude <= {north} AND lattitude >= {south} AND longitude <= {east} AND longitude >= {west}) pc
  ON pp.postcode = pc.postcode""")

  cols= [column[0] for column in cur.description] 
  rows = cur.fetchall()

  return rows, cols, north, south, east, west

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