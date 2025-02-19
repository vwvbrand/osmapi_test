[The main script](ohsome_experiment.py) fetches spatial features from OpenStreetMap database (based on custom queries) through ohsome API and produces stats on the time of fetching spatial features in GeoJSON format by the same bounding boxes.
Sample input datasets are:
- [MUCSC](https://polipapers.upv.es/index.php/raet/article/view/13112), covering Catalonia (Spain). Presented just as a bounding box as actual values in input datasets are not needed.
- [ESA Sentinel-2](https://collections.sentinel-hub.com/impact-observatory-lulc-map/), covering parts of England.
**Names of bounding boxes are currently hardcoded

To switch on/off input datasets and yearstamps, feel free to edit [the configuration file](config_ohsome_history.yaml). Paths and filenames can be also edited there.

To check outputs, see the [log](logs/ohsome.txt).