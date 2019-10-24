import pandas as pd

# from .buoy_data_scraper import BuoyDataScraper
from .realtime_scraper import RealtimeScraper
from .historical_scraper import HistoricalScraper


def load_realtime(station_id=None, dtype=None, path=None, timezone='UTC'):
    if not path:
        if not station_id or not dtype:
            raise Exception("If path is not specified, must provide station_id and dtype for default path finding.")
        path = "data/{}/realtime/{}.pkl".format(station_id, dtype)
    try:
        data = pd.read_pickle(path)
    except:
        print("No pickle at {}".format(path))
    try:
        return data.tz_convert(timezone)
    except:
        print("{} is not a valid timezone. See https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.Series.dt.tz_convert.html")

def load_historical(station_id=None, dtype=None, path=None, timezone='US/Pacific'):
    if not path:
        if not station_id or not dtype:
            raise Exception("If path is not specified, must provide station_id and dtype for default path finding.")
        path = "data/{}/historical/{}.pkl".format(station_id, dtype)
    try:
        data = pd.read_pickle(path)
    except:
        print("No pickle at {}".format(path))
    try:
        return data.tz_convert(timezone)
    except:
        print("{} is not a valid timezone. See https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.Series.dt.tz_convert.html")
