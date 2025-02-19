import os
import sys
import requests
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO
import ssl
from requests.adapters import HTTPAdapter
# import urllib3
from urllib3.poolmanager import PoolManager
# import certifi
import logging
import yaml
# import json
import geojson
import warnings
from itertools import product  # import product for Cartesian product of lists
import rasterio
from pyproj import Transformer
import time

start_time = time.time()
cwd = os.getcwd()
print(f"The current working directory is: {cwd}")


"""
EXPERIMENTS ON OHSOME AND OVERPASS TURBO API

INPUT
- Input raster datasets to extract spatial extent of area analysed (GeoTIFF)
- YAML file with the configuration parameters

OUTPUT 
- Spatial features of interest from OSM by various timestamps (GeoJSON?).

ISSUES AND LIMITATIONS:
- fetching OSM data is quick for small bbox in England (11.5 s from three timestamps) but transformation from JSON to GeoJSON is long
- but POST method of API is already exporting it as GEOJSON, so the total time with export is around 31s
- if specify all attributes (data) fetched then time increases to 26 and 85s respectively
- translation to geopackage is still needed (?) - it operates faster (through geojson2gpkg)

- no info on data and time limit
"""

# setup logging
logging.basicConfig(level=logging.INFO) # for more verbosity use 'DEBUG'
logger = logging.getLogger(__name__)
# save to log
file_handler = logging.FileHandler('logs/ohsome.log', mode='w')
file_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s') # to include time/name/level/message
formatter = logging.Formatter('%(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# custom TLS Adapter to enforce TLSv1.2
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.options |= ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1  # enforce TLSv1.2+
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

session = requests.Session()
session.mount('https://', TLSAdapter())

# load filename and year from the configuration file
with open('config_ohsome_history.yaml', 'r') as f:
    config = yaml.safe_load(f)

years = config.get('year')
if not isinstance(years, list):
    years = [years]
logger.info(f"Yearstamps of input raster datasets:{years}")

if not years or any(year is None for year in years):
    warnings.warn("Year variable is not found or is None in the configuration file.")

lulc_templates = config.get('lulc')
if not isinstance(lulc_templates, list):
    lulc_templates = [lulc_templates]

if not lulc_templates or any(lulc is None for lulc in lulc_templates):
    warnings.warn("LULC template is not found or is None in the configuration file.")

# generate lulc filenames for each combination of lulc template and year
lulc_year_combinations = [(template.format(year=year), template, year) for template, year in product(lulc_templates, years)]

logger.info(f"Input rasters to be used for processing: {', '.join([name for name, _, _ in lulc_year_combinations])}")

# specify parent directory
parent_dir = os.getcwd()  # automatically extract current folder to avoid hard-coded path.
print(f"Parent directory: {parent_dir}")

# add Python path to search for scripts, modules
sys.path.append(parent_dir)

# specify paths
lulc_dir = config.get('lulc_dir')
impedance_dir = config.get('impedance_dir')
vector_dir = config.get('vector_dir')
output_dir = config.get('output_dir')

# extract names of bounding boxes
bbox_1_name = config.get('bbox_1_name')
bbox_2_name = config.get('bbox_2_name')

# create the output directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Created directory: {output_dir}")

# initialize bounding box parameter string for the further request
bbox_params = []

# process each lulc derived from different years
for lulc, template, year in lulc_year_combinations:
    lulc_path = os.path.join(parent_dir, lulc_dir, lulc)

    # normalize paths (to avoid mixing of backslashes and forward slashes)
    lulc_path = os.path.normpath(lulc_path)
    print(f"Path to the input raster dataset: {lulc_path}")

    # call external module to reproject input raster dataset (from config)
    try:
        with rasterio.Env(GTIFF_SRS_SOURCE="EPSG"):
            with rasterio.open (lulc_path) as src:
                bounds = src.bounds
                print (f"Bbox before CRS transformation: {bounds}")
                src_crs = src.crs
                out_crs = "EPSG:4326"

                # create a transformer
                transformer = Transformer.from_crs(src_crs, out_crs, always_xy=True)
                # transform to WGS84
                min_lon, min_lat = transformer.transform(bounds.left, bounds.bottom)
                max_lon, max_lat = transformer.transform(bounds.right, bounds.top)
    
                print(f"Bounds in WGS84: min lon: {min_lon}, min lat: {min_lat}, max lon: {max_lon}, max lat: {max_lat}")
        
                # naming the bounding box (without year)
                template_name = f"bbox_{os.path.splitext(os.path.basename(template))[0]}"
            
                # TODO - replace with a more flexible name (hardcoded now)
                if '_esa' in template_name.lower():
                    bbox_name = bbox_1_name # or "UK, ESA"
                else:
                    bbox_name = bbox_2_name # or "Catalonia" (in this case - mock raster with the specs as original LULC raster but value=1 in all pixels to avoid licence issues)

                # Create bbox as a dictionary
                bbox_entry = {
                    "name": bbox_name,
                    "bbox": [min_lon, min_lat, max_lon, max_lat]  # Store as a list of values
                }

                # naming the bounding box (without year)
                template_name = f"bbox_{os.path.splitext(os.path.basename(template))[0]}"
        
            logger.info(f"Bounding box for {bbox_name}: {bbox_entry}")
            print("-" * 40)

        # append bbox with name to parameters
        bbox_params.append(bbox_entry)

    except Exception as e:
        print(f"Failed to transform {lulc_path}: {e}")
        print("-" * 40)

    # hard-coded names of bounding boxes
    
# comnine into one dictionary
bboxes = {entry["name"]: entry["bbox"] for entry in bbox_params}
print("Bounding boxes:")
print(bboxes)

# define API endpoint
url_ohsome = 'https://api.ohsome.org/v1/elements/geometry'

# defining parameters
bbox_name = bbox_1_name
bbox_value = bboxes[bbox_name]
bbox_str = ",".join(map(str, bbox_value)) # flatten to string
print(f"Using bounding box: {bbox_str}")
properties = "metadata,tags" # to include all attributes
showMetadata = "true"
timeout = 600

# define timestamps
timestamps = ",".join(f"{year}-12-31" for year in years) # create a timestamp with the last day of each year
# timestamps = "2012-12-31,2017-12-31,2022-12-31" # list of timestamps, not intervals!

for i in range(3):  # separate log blocks
    logger.info(f"*" * 40)
logger.info(f"Timestamps considered: {timestamps}")
logger.info(f"-" * 40)

# filters
filter_roads = "type:way and (highway in (motorway, motorway_link, trunk, trunk_link, primary, primary_link, secondary, secondary_link, tertiary, tertiary_link))"
filter_railways = "type:way and (railway in (rail, light_rail, narrow_gauge, tram, preserved))"
filter_waterways = "type:way and (waterway in (river, canal, flowline, tidal_channel) or water in (river, canal))"
filter_waterbodies = "natural=water or (water in (cenote, lagoon, lake, oxbow, rapids, river, stream, stream_pool, canal, harbour, pond, reservoir, wastewater, tidal, natural)) or (landuse=reservoir) or (waterway=riverbank)"
filter_vineyards = "landuse=vineyard" # now: do not consider filter by type/geometry as might be mapped as a single node (https://wiki.openstreetmap.org/wiki/Tag:landuse=vineyard)

# TODO - for filter_roads consider geometry:line filter 

# map variable names to their values
filters = {
    "filter_roads": filter_roads,
    "filter_railways": filter_railways,
    "filter_waterways": filter_waterways,
    "filter_waterbodies": filter_waterbodies,
    "filter_vineyards": filter_vineyards,
}

# TODO - print number of attributes in each response (tags and metadata beginning with @)

params = [
    {
    "bboxes": bbox_str,
    "showMetadata": showMetadata,
    "properties": properties,
    "time": timestamps, 
    "filter": filter_roads
    },
    {
    "bboxes": bbox_str,
    "showMetadata": showMetadata,
    "properties": properties,
    "time": timestamps,
    "filter": filter_railways
    },
    {
    "bboxes": bbox_str,
    "showMetadata": showMetadata,
    "properties": properties,
    "time": timestamps,
    "filter": filter_waterways
    },
    {
    "bboxes": bbox_str,
    "showMetadata": showMetadata,
    "properties": properties,
    "time": timestamps,
    "filter": filter_waterbodies
    },
    {
    "bboxes": bbox_str,
    "showMetadata": showMetadata,
    "properties": properties,
    "time": timestamps,
    "filter": filter_vineyards
    },
]

# loop over queries
for param_set in params:
    query_start = time.time()
    filter_value = param_set["filter"]
    filter_name = next(
        (name for name, value in filters.items() if value == filter_value), "unknown_filter"
    )
    logger.info(f"Filter variable name: {filter_name}")
    try:
        response = session.post(url_ohsome, data=param_set, timeout=timeout)  # use 'data' instead of 'params'
        logger.info(f"Response status code: {response.status_code}") 
        '''print(response.json())''' # DEBUG
        # if successful, save
        if response.status_code == 200:
            response_data = response.json()
            # count features
            feature_count = len(response_data.get("features",[])) # how many values corresponds to "features" key
            
            # get the features list
            features = response_data.get("features", [])
            unique_attributes = set()

            for feature in features:
                # collect keys from properties, excluding those starting with '@'
                unique_attributes.update(
                    key for key in feature.get("properties", {}).keys()
                    if not key.startswith("@")
                )

            logger.info(f"Total unique attributes across all features, excluding metadata (@): {len(unique_attributes)}")
            for attr in sorted(unique_attributes):  # sort attributes for easier reading
                logger.info(f" - {attr}")

            
            # to geojson
            output_filename = os.path.join(output_dir, f"{filter_name}.geojson")
            with open(output_filename, 'w') as geojson_file:
                geojson.dump(response_data, geojson_file)
            logger.info(f"GeoJSON has been saved to {output_filename}")
            
    
    except requests.RequestException as e:
        logger.error(f"Request failed for params: {param_set}. Error: {e}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

    query_finish = time.time()
    query_time = query_finish - query_start
    logger.info(f"Query time: {query_time}")
    logger.info(f"Number of features: {feature_count}")
    logger.info("-" * 40)