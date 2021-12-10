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
import math
from . import access
from . import assess

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.linear_model import PoissonRegressor

def get_properties_bounding_boxw_dist(lat,lon,dist,pt,date):
  up = dist/111
  across = dist/(111*math.cos(math.radians(lat)))
  north = lat + up
  south = lat - up
  east = lon + across
  west = lon - across

  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
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

def get_properties_bounding_dist(lat,lon,dist,pt,date):
  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
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
              HAVING distance<{dist}) pc
  ON pp.postcode = pc.postcode""")
  cols= [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols

def get_distance_to_point(lat, lon, pdf):
  pdf["lat"] = pd.to_numeric(pdf["lat"])
  pdf["lon"] = pd.to_numeric(pdf["lon"])
  pdf["distance"] = pdf.apply(lambda row : assess.get_distance(lat, lon, row["lat"], row["lon"]), axis=1)
  return pdf

def get_weights(weight_cols, w, d):
  date = pd.to_datetime(d)
  weight1 = pd.DataFrame(data=w, columns=weight_cols)
  weight1["date_diff"] = weight1["date_int"]-(date.value/10**9)
  weight1["date_diff"] = 1 - weight1["date_diff"].abs()/weight1["date_diff"].abs().max()
  weight1["distance_w"] = 1 - weight1["distance"]/weight1["distance"].max()
  weight1["combined"] = weight1["date_diff"]*weight1["distance_w"]
  weight1["combined"] = weight1["combined"]**0.5
  return weight1[["date_diff", "distance_w", "combined"]]

def get_prediction_data(lat, lon, date, kinds, sizes, dist):
  pcs1 = pd.DataFrame(data=[["XX", lat, lon]], columns=["postcode", "lat", "lon"])
  to_predict, _ = assess.get_features_from_list(pcs1, lat, lon, kinds, sizes,dist)
  date = pd.to_datetime("2018-01-01")
  to_predict["date_int"] = pd.to_datetime(date).value/10**9
  to_predict["lat"]=lat
  to_predict["lon"]=lon
  return to_predict


def make_prediction(lat, lon, date, pt):
  dist, kinds, sizes = (5, ["schools", "public_transport","parks_and_rec", "services", "rural"],[1,1,1,1,1])
  props = assess.get_props_with_features(lat, lon, date, pt, dist, kinds, sizes)
  props = get_distance_to_point(lat,lon,props)


  features = list(props.columns)
  for x in ['price', 'date_int', 'lat', 'lon','distance']:
    features.remove(x)
  
  weight_cols = ['date_int', 'distance']
  x = props[features].to_numpy()
  y = props["price"].to_numpy()
  w = props[weight_cols].to_numpy()
  
  ss = StandardScaler()
  x = ss.fit_transform(x)

  pca = PCA(0.9)
  prin_comp = pca.fit_transform(x)

  new_props = pd.DataFrame(data = prin_comp, columns=["component_"+str(x) for x in range(prin_comp.shape[1])])
  columns = new_props.columns

  new_props = pd.concat([new_props, props[["price"]]], axis=1)

  weight1 = get_weights(weight_cols, w, date)

  to_pred = get_prediction_data(lat, lon,date, kinds, sizes, dist)

  x1= new_props[columns].to_numpy()

  m_poisson_comb = PoissonRegressor().fit(x1,y, sample_weight=weight1["combined"].to_numpy())


  xtopred = to_pred[features].to_numpy()


  xtopred = ss.transform(xtopred)
  to_pred = pca.transform(xtopred)

  pred_price_comb = m_poisson_comb.predict(to_pred)
  comb_pred = pred_price_comb[0]

  print("prediction: £",comb_pred)