# This file contains code for suporting addressing questions in the data

"""# Here are some of the imports we might expect 
import sklearn.model_selection  as ms
import sklearn.linear_model as lm
import sklearn.svm as svm
import sklearn.naive_bayes as naive_bayes
import sklearn.tree as tree

import GPy
import torch
import tensorflow as tf

# Or if it's a statistical analysis
import scipy.stats"""

"""Address a particular question that arises from the data"""

def get_properties_bounding_boxw_dist(lat,lon,dist,pt,date):
  up = (dist/11.1)*0.1
  across = (dist*0.1)/(11.1*math.cos(math.radians(lat)))
  north = lat + up
  south = lat - up
  east = lon + across
  west = lon - across

  conn = create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(f"""
  SELECT pp.transaction_unique_identifier as tui, pp.price as price, pp.date_of_transfer as date, pp.property_type as type, pp.primary_addressable_object_name as pao,	pp.secondary_addressable_object_name as sao,	pp.street as street, pp.locality as locality,	pp.town_city as town_city,	pp.district as district,	pp.county as county, pp.postcode as postcode, pc.lattitude as lat,	pc.longitude as lon,	pc.postcode_sector as postcode_sector, pc.distance as distance
  FROM (SELECT * FROM `pp_data` WHERE property_type = '{pt}' AND DATEDIFF('{date}', date_of_transfer) < 365*5 AND date_of_transfer < '{date}') pp
  INNER JOIN (SELECT 
              postcode, postcode_sector, lattitude, longitude,
              (6371 *
                acos(cos(radians({lat})) * 
                cos(radians(lattitude)) * 
                cos(radians(longitude) - 
                radians({lon})) + 
                sin(radians({lat})) * 
                sin(radians(lattitude)))
              ) AS distance 
              FROM `postcode_data` 
              WHERE lattitude <= {north} AND lattitude >= {south} AND longitude <= {east} AND longitude >= {west}) pc
  ON pp.postcode = pc.postcode""")
  cols= [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols, north, south, east, west