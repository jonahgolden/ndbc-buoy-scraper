from .buoy import Buoy
import pandas as pd

STATIONS = {"Neah Bay": {"id":"46087"}, 
            "New Dungeness": {"id":"46088"}}
    
def save_buoy_data(station_id, latitude="", longitude="", filename_prefix="", verbose=True):
    buoy = Buoy(station_id, latitude, longitude)
    buoy.get_historical_data(save=True, filename_prefix=filename_prefix, verbose=verbose)

def load_data(station=None, dtype=None, path=None, data_dir="../data/{}/{}{}.pkl", filename_prefix=""):
    if not (path or (station and dtype)):
        raise Exception("Must provide either path to data or station and dtype.")
    pd.read_pickle(data_dir.format(STATIONS[station], filename_prefix, dtype))
