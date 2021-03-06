from .config import *

from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

import pandas as pd
import numpy as np
import osmnx as ox
import geopandas as gpd
import matplotlib.pyplot as plt
import mlai.plot as plot

# --------------------------------------------------------------------------------------------
# LOOKING AT MISSING DATA
# --------------------------------------------------------------------------------------------

def select_top_wcols(conn, table,  n):
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    username, password, url = access.get_credentials_from_file()
    conn = access.create_connection(username, password, url, "arw92-database")
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM `{table}` LIMIT {n}')
    # cur.execute(f'SELECT * FROM `{table}`;')

    cols = [column[0] for column in cur.description] 
    rows = cur.fetchall()
    return rows, cols

def count_all(table):
  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  #   This count includes null values
  cur.execute(f'SELECT COUNT(*) as total FROM `{table}`')
  cols = [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols

def null_count(table, column, total):
  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(f"SELECT {total}-COUNT('{column}') As {column} FROM {table};")
  cols = [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols

def empty_count(table, column, id):
  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(f"SELECT COUNT('{id}') As total FROM {table} WHERE {column} = '';")
  cols = [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols

def sel_empty(table, col):
  username, password, url = access.get_credentials_from_file()
  conn = access.create_connection(username, password, url, "arw92-database")
  cur = conn.cursor()
  cur.execute(f"SELECT *  FROM `{table}` WHERE {col} = ''")
  cols = [column[0] for column in cur.description] 
  rows = cur.fetchall()
  return rows, cols


# --------------------------------------------------------------------------------------------
# EXPLORING PRICE PAID DATA
# --------------------------------------------------------------------------------------------

def convert_to_gdf(pdf):
  geometry=gpd.points_from_xy(pdf.lon, pdf.lat)
  gdf = gpd.GeoDataFrame(pdf, geometry=geometry)
  gdf.crs = "EPSG:4326"
  return gdf

def plot_map(north, south, east, west, name, points, alpha, title, colour_col=None):
  graph = ox.graph_from_bbox(north, south, east, west)

  # Retrieve nodes and edges
  nodes, edges = ox.graph_to_gdfs(graph)

  # Get place boundary related to the place name as a geodataframe
  area = ox.geocode_to_gdf(name)

  fig, ax = plt.subplots(figsize=plot.big_figsize)

  # Plot the footprint
  area.plot(ax=ax, facecolor="white", zorder=1)

  # Plot street edges
  edges.plot(ax=ax, linewidth=1, edgecolor="dimgray", zorder=2)

  ax.set_xlim([west, east])
  ax.set_ylim([south, north])
  ax.set_xlabel("longitude")
  ax.set_ylabel("latitude")
  ax.title.set_text(title)

  if colour_col is None:
      points.plot(ax=ax, alpha=alpha, markersize=5, zorder=3, legend=True)
  else:
      points.plot(ax=ax, column=colour_col, cmap='plasma', alpha=alpha, markersize=5, zorder=3, legend=True)




def plot_pt_price_hist(pdf):
  D = pdf[pdf["type"] == 'D']["price"]
  S = pdf[pdf["type"] == 'S']["price"]
  T = pdf[pdf["type"] == 'T']["price"]
  F = pdf[pdf["type"] == 'F']["price"]
  O = pdf[pdf["type"] == 'O']["price"]

  bins = np.linspace(start=0, num=50, stop=pdf["price"].max())

  plt.hist(D, bins, alpha=0.2, label='D',histtype="stepfilled")
  plt.hist(S, bins, alpha=0.2, label='S',histtype="stepfilled")
  plt.hist(T, bins, alpha=0.2, label='T',histtype="stepfilled")
  plt.hist(F, bins, alpha=0.2, label='F',histtype="stepfilled")
  plt.hist(O, bins, alpha=0.2, label='O',histtype="stepfilled")
  plt.legend(loc='upper right')
  plt.show()


def remove_price_first_percentiles(pdf):
  return pdf[(pdf["price"]>pdf["price"].quantile(0.001)) & (pdf["price"]<pdf["price"].quantile(0.99))]

def plot_price_map(north, south, east, west, name, points, alpha):
  graph = ox.graph_from_bbox(north, south, east, west)

  # Retrieve nodes and edges
  nodes, edges = ox.graph_to_gdfs(graph)

  # Get place boundary related to the place name as a geodataframe
  area = ox.geocode_to_gdf(name)

  fig, ax = plt.subplots(figsize=plot.big_figsize)

  # Plot the footprint
  area.plot(ax=ax, facecolor="white", zorder=1)

  # Plot street edges
  edges.plot(ax=ax, linewidth=1, edgecolor="dimgray", zorder=2)

  ax.set_xlim([west, east])
  ax.set_ylim([south, north])
  ax.set_xlabel("longitude")
  ax.set_ylabel("latitude")

  # Plot all points, not including other property type to get a better price scale
  new_points = points[points["type"]!='O']
  new_points.plot(ax=ax, column='price',cmap='plasma', alpha=alpha, markersize=5, zorder=3, legend=True)

def plot_each_kind(n,s,e,w,name, pois_list, kinds, alpha):
  graph = ox.graph_from_bbox(n, s, e, w)

  # Retrieve nodes and edges
  nodes, edges = ox.graph_to_gdfs(graph)

  # Get place boundary related to the place name as a geodataframe
  area = ox.geocode_to_gdf(name)

  fig, axs = plt.subplots(nrows=len(kinds), figsize=(10,10*len(kinds)) )
  fig.tight_layout()
  for i in range(len(kinds)):
    # Plot the footprint
    area.plot(ax=axs[i], facecolor="white", zorder=1)

    # Plot street edges
    edges.plot(ax=axs[i], linewidth=1, edgecolor="dimgray", zorder=2)

    axs[i].set_xlim([w, e])
    axs[i].set_ylim([s, n])
    axs[i].set_xlabel("longitude")
    axs[i].set_ylabel("latitude")
    axs[i].title.set_text(kinds[i])

    # Plot all POIs 
    pois_list[i].plot(ax=axs[i], alpha=alpha, markersize=5, zorder=3, legend=True)

def plot_each_feature(n,s,e,w,name, points, kinds, alpha):
  # this method assumes there are columns corresponding to all kinds
  graph = ox.graph_from_bbox(n, s, e, w)

  # Retrieve nodes and edges
  nodes, edges = ox.graph_to_gdfs(graph)

  # Get place boundary related to the place name as a geodataframe
  area = ox.geocode_to_gdf(name)

  fig, axs = plt.subplots(nrows=len(kinds),ncols=2, figsize=(10,10*len(kinds)) )
  fig.tight_layout()
  for i in range(len(kinds)):
    # Plot the footprint
    area.plot(ax=axs[i][0], facecolor="white", zorder=1)
    area.plot(ax=axs[i][1], facecolor="white", zorder=1)

    # Plot street edges
    edges.plot(ax=axs[i][0], linewidth=1, edgecolor="dimgray", zorder=2)
    axs[i][0].set_xlim([w, e])
    axs[i][0].set_ylim([s, n])
    axs[i][0].set_xlabel("longitude")
    axs[i][0].set_ylabel("latitude")
    axs[i][0].title.set_text("n_"+kinds[i])

    edges.plot(ax=axs[i][1], linewidth=1, edgecolor="dimgray", zorder=2)
    axs[i][1].set_xlim([w, e])
    axs[i][1].set_ylim([s, n])
    axs[i][1].set_xlabel("longitude")
    axs[i][1].set_ylabel("latitude")
    axs[i][1].title.set_text("d_"+kinds[i])

    # Plot all POIs 
    points.plot(ax=axs[i][0], column="n_"+kinds[i], cmap="plasma",alpha=alpha, markersize=5, zorder=3, legend=True)
    points.plot(ax=axs[i][1], column="d_"+kinds[i], cmap="plasma",alpha=alpha, markersize=5, zorder=3, legend=True)

# --------------------------------------------------------------------------------------------
# GENERATING FEATURES
# --------------------------------------------------------------------------------------------
# TODO: Explain the function of all of these functions
def get_unique_postcodes(prop):
  '''
  Gets a dataframe of just the unique postcodes of the properties, and their 
  respective lat/lon. 
  '''
  postcodes = prop[["postcode", "lat", "lon"]].copy()

  # Squashes duplicates and removes postcodes with missing lat/lon
  postcodes = postcodes.drop_duplicates()[(postcodes["lat"]!='')&(postcodes["lon"]!='')].copy()
  postcodes["lat"] = pd.to_numeric(postcodes["lat"])
  postcodes["lon"] = pd.to_numeric(postcodes["lon"])
  return postcodes

def get_distance(lat1, lon1, lat2, lon2):
  return (6371 * np.arccos(np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.cos(np.radians(lon2) - np.radians(lon1)) + np.sin(np.radians(lat1)) * np.sin(np.radians(lat2))))

def get_nearest(lat,lon,pois, dist):
  # raises exception if no poi within dist
  pois_dist = pois.copy()
  pois_dist["dist"] = pois_dist.apply(lambda row : get_distance(lat,lon,row["lat"],row["lon"]),axis=1)
  if pois_dist.shape[0]<1:
    raise Exception("No pois within range.")
  return pois_dist["dist"].min()

#  These methods get the centres of polygons/shapes of pois for use when calculating features
def get_centres_lat(shape):
  centre = 0
  if shape.geom_type == "Polygon" or shape.geom_type == "MultiPolygon":
    centre = shape.centroid.y
  elif shape.geom_type == "Point" or shape.geom_type == "MultiPoint":
    centre = shape.y
  return centre

def get_centres_lon(shape):
  centre = 0
  if shape.geom_type == "Polygon" or shape.geom_type == "MultiPolygon":
    centre = shape.centroid.x
  elif shape.geom_type == "Point" or shape.geom_type == "MultiPoint":
    centre = shape.x
  return centre

def get_coords_of_pois(pois):
  pois["lat"] = pois.apply(lambda row : get_centres_lat(row["geometry"]), axis=1)
  pois["lon"] = pois.apply(lambda row : get_centres_lon(row["geometry"]), axis=1)
  return pois

def count_pois(lat,lon, pois, dist):
  pois_dist = pois.copy()
  pois_dist["dist"] = pois_dist.apply(lambda row : get_distance(lat,lon,row["lat"],row["lon"]),axis=1)
  p_in = pois_dist[pois_dist["dist"]<dist]
  return p_in.shape[0]
  
def get_feature_for_postcodes(pcs, pois, kind, size, dist):
  pcs[f"n_{kind}"] = pcs.apply(lambda row : count_pois(row["lat"], row["lon"], pois, size), axis=1)

  # added the distance too, not edited line above so if doesn't work I can remove
  pcs[f"d_{kind}"] = pcs.apply(lambda row : get_nearest(row["lat"], row["lon"], pois, dist),axis=1)
  return pcs

def get_feature(pcs,lat,lon, kind, size, dist):
  pois = access.features_around_point_bbox(lat,lon,2*dist, kind)

  if pois.shape[0] > 0:
    pois = get_coords_of_pois(pois)
    pcs = get_feature_for_postcodes(pcs, pois, kind, size, dist)
    return pcs
  else:
    # Don't give any values to column if no pois of that kind in area
    return None

def get_features_from_list(pcs, lat, lon, kinds, sizes, dist):
  if len(sizes)!=len(kinds):
    raise Exception("Differing number of kinds and sizes")

  names = []
  for i in range(len(kinds)):
    pcs_temp = get_feature(pcs.copy(),lat, lon, kinds[i],sizes[i], dist)
    if pcs_temp is None:
      continue
    else:
      pcs = pcs_temp.copy()
      names.append("n_"+kinds[i])
      names.append("d_"+kinds[i])
  
  return pcs[["postcode"]+names].copy(), names

def get_features(prop, lat, lon, kinds, sizes, dist):
  pcs = get_unique_postcodes(prop)

  pcs, names = get_features_from_list(pcs, lat, lon, kinds, sizes, dist)

  prop_feat = pd.merge(prop, pcs, how="inner", on="postcode")

  return prop_feat, names

#  Lat, lon, date, pt and sizes introduced here purely to limit size of the dataset that we're collecting
def get_props_with_features(lat, lon, date, pt, dist, kinds, sizes):
  r, c, new_dist = access.get_properties_within_dist_type_date(lat, lon, dist, pt, date)
  prop = pd.DataFrame.from_records(data=r, columns=c)

  if prop.shape[0] > 0:
    prop_feat, names = get_features(prop, lat, lon, kinds, sizes, new_dist)
    prop_feat['date_int'] = pd.to_datetime(prop_feat['date']).astype(int)/10**9
    return prop_feat[["price", "date_int", "lat", "lon"]+names].copy(), new_dist
  else:
    return None

# ---------------------------------------------------------------------------------------------
# The bounding box method is a bit faster but features are less accurate and the code is messy
# def get_bbox_around_postcodes(pcs, size):
#   pcs["north"] = pcs["lat"]+(size/111)
#   pcs["south"] = pcs["lat"]-(size/111)
#   pcs["east"] = pcs["lon"]+((size)/(111*np.cos(np.radians(pcs["lat"]))))
#   pcs["west"] = pcs["lon"]-((size)/(111*np.cos(np.radians(pcs["lat"]))))
#   return pcs

# def get_distance(lat1, lon1, lat2, lon2):
#   """This formula for distance was taken from """
#   return (6371 * np.arccos(np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.cos(np.radians(lon2) - np.radians(lon1)) + np.sin(np.radians(lat1)) * np.sin(np.radians(lat2))))

# def get_nearest(lat,lon,n,s,e,w,pois):
#   pois_dist = pois[(pois["lat"]<=n) & (pois["lat"]>=s) & (pois["lon"]<=e) & (pois["lon"]>=w)].copy()
#   count = 1
#   while pois_dist.shape[0]<1:
#     n,s,e,w = (lat+(n-lat)*(1.41**count)), (lat-(lat-s)*(1.41**count)),(lon+(e-lon)*(1.41**count)),(lon-(lon-w)*(1.41**count))
#     pois_dist = pois[(pois["lat"]<=n) & (pois["lat"]>=s) & (pois["lon"]<=e) & (pois["lon"]>=w)].copy()
#   pois_dist["dist"] = pois_dist.apply(lambda row : get_distance(lat,lon,row["lat"],row["lon"]),axis=1)
#   return pois_dist["dist"].min()

# def get_centres_lat(shape):
#   centre = 0
#   if shape.geom_type == "Polygon" or shape.geom_type == "MultiPolygon":
#     centre = shape.centroid.y
#   elif shape.geom_type == "Point" or shape.geom_type == "MultiPoint":
#     centre = shape.y
#   return centre

# def get_centres_lon(shape):
#   centre = 0
#   if shape.geom_type == "Polygon" or shape.geom_type == "MultiPolygon":
#     centre = shape.centroid.x
#   elif shape.geom_type == "Point" or shape.geom_type == "MultiPoint":
#     centre = shape.x
#   return centre

# def get_coords_of_pois(pois):
#   pois["lat"] = pois.apply(lambda row : get_centres_lat(row["geometry"]), axis=1)
#   pois["lon"] = pois.apply(lambda row : get_centres_lon(row["geometry"]), axis=1)
#   return pois

# def count_pois(n, s, e, w, pois):
#   if pois.shape[0]>0:
#     p_in = pois[(pois["lat"]<=n) & (pois["lat"]>=s) & (pois["lon"]<=e) & (pois["lon"]>=w)]
#     return p_in.shape[0]
#   else:
#     return 0

# def get_counts_for_postcodes(pcs, pois, name):
#   pcs[f"n_{name}"] = pcs.apply(lambda row : count_pois(row["north"], row["south"], row["east"], row["west"], pois), axis=1)

#   # added the distance too, not edited line above so if doesn't work I can remove
#   pcs[f"d_{name}"] = pcs.apply(lambda row : get_nearest(row["lat"],row["lon"], row["north"], row["south"], row["east"], row["west"], pois),axis=1)
#   return pcs
  
# def get_counts(pcs,lat,lon, name, size):
#   name_not_in = False
#   pcs_bbox = get_bbox_around_postcodes(pcs, size)
#   n, s, e, w = pcs_bbox["north"].max(), pcs_bbox["south"].min(), pcs_bbox["east"].max(), pcs_bbox["west"].min()
  
#   tags = access.get_tags(name)
#   pois = ox.geometries_from_bbox(n, s, e, w, tags)
#   if pois.shape[0] > 0:
#     pois = get_coords_of_pois(pois)
#     pcs_bbox = get_counts_for_postcodes(pcs_bbox, pois, name)
#   else:
#     # Don't give any values to column if no pois of that kind in area
#     name_not_in = True

#   pn = lat+(size/111)
#   ps = lat-(size/111)
#   pe= lon+((size)/(111*np.cos(np.radians(lat))))
#   pw = lon-((size)/(111*np.cos(np.radians(lat))))

#   to_predict = count_pois(pn, ps, pe, pw, pois)
#   return pcs_bbox, to_predict, name_not_in

# def get_counts_list(pcs, lat, lon, kinds, sizes):
#   if len(sizes)!=len(kinds):
#     raise Exception("Differing number of kinds and sizes")

#   to_predict=[]
#   kinds_not_in =[]
#   for i in range(len(kinds)):
#     pcs, to_pred, kind_not_in = get_counts(pcs,lat, lon, kinds[i],sizes[i])
#     if not kind_not_in:
#       to_predict.append(to_pred)
#     else:
#       kinds_not_in.append(kinds[i])
  
#   names = ["n_"+name for name in kinds if name not in kinds_not_in]+["d_"+name for name in kinds if name not in kinds_not_in]

#   return pcs[["postcode"]+names].copy(), names, to_predict

# def get_features(prop, lat, lon, kinds, sizes):
#   pcs = get_unique_postcodes(prop)
#   pcs["lat"] = pd.to_numeric(pcs["lat"])
#   pcs["lon"] = pd.to_numeric(pcs["lon"])

#   pcs, names, to_pred = get_counts_list(pcs, lat, lon, kinds, sizes)

#   prop_counts = pd.merge(prop, pcs, on="postcode")

#   return prop_counts, names, to_pred
  
# def get_props_with_counts(lat, lon, date, pt, dist, kinds, sizes):
#   r, c, n, s, e, w = access.get_properties_bounding_box(lat, lon, dist, pt, date)

#   prop = pd.DataFrame.from_records(data=r, columns=c)

#   # drop those that don't have a lat and lon
#   prop = prop[(prop["lat"]!='') & (prop["lon"]!='')].copy()
#   if prop.shape[0] > 0:
#     prop_counts, names,_ = get_features(prop, lat, lon, kinds, sizes)
#     prop_counts['date_int'] = pd.to_datetime(prop_counts['date']).astype(int)/10**9
#     return prop_counts[["price", "date_int", "distance", "lat", "lon"]+names].copy()
#   else:
#     return None



