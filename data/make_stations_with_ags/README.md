Hystreet delivers only city and street name for each station. However, we need to know the location and the AGS. The `make_stations_with_ags.py` script finds the approximate position by online lookup (Nominatim) and then finds the AGS by nearest-neighbour search of each "Gemeinde" in `data-gemeindeschluessel.csv`.

Required input file: `data-gemeindeschluessel.csv`

Output file: `stations_with_ags.csv`

This needs to be run only once in a while, that is whenever hystreet adds new stations.

The `prototype_make_stations_with_ags` jupyter notebook can be used to interactively tweak the script and contains some validation tests for the assigned locations.