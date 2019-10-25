"""
Jonah Golden, 2019
Buoy Class -- Handles all data for NDBC Buoys (https://www.ndbc.noaa.gov/)
Creates pandas dataframe objects for realtime and historical data.

Initialize a Buoy object:
    Inputs :
        buoy_id (int or string) : valid buoy id's can be found here: https://www.ndbc.noaa.gov/
        data_dir (string) : Optional, directory to save data to. Default is "buoydata/"
    Output :
        Buoy object

Dataframes are returned (single data types only) via two methods that can be called on Buoy Objects:
    1. `get_realtime`
        Input : dtype (string)
        Output : pandas dataframe with realtime data (most recent 45 days)
    2. `get_historical`
        Inputs :
            dtype (string)
            Up to one of the following:
                - year (int) : Optional in range [2007, LastYear]. If specified, returns data for just that year.
                - month (int) : Optional in range [1, 12]. If specified, returns data for just that month this year.
                * Note: if neither year or month is provided, will return data for all available years and months.
        Output : pandas dataframe with historical data

Dataframes are pickled (one to many data types) via two methods that can be called on Buoy Objects:
    1. `save_realtime`
        Input :
            dtypes (List of strings) : Optional, list of dtypes to save data for. Default is all available.
        Output :
            Pickles dataframes to "<data_dir>/<buoy_id>/realtime/<dtype>.pkl"
    2. `save_historical`
        Input :
            dtypes (List of strings) : Optional, list of dtypes to save data for. Default is all available.
        Output :
            Pickles dataframes to "<data_dir>/<buoy_id>/historical/<dtype>.pkl"

Notes:
    * Current implementation scrapes historical data from 2007 through most recent month.
        Data before 2007 was formatted slightly differently, and will require some tweaking of functions.
"""

import pandas as pd
import re   # For metadata
from .realtime_scraper import RealtimeScraper
from .historical_scraper import HistoricalScraper

class Buoy:

    def __init__(self, buoy_id, data_dir="buoydata/"):
        self.buoy_id = str(buoy_id)
        self.data_dir = data_dir
        self._populate_metadata(self.buoy_id)
        self.realtime = RealtimeScraper(self.buoy_id, data_dir)
        self.historical = HistoricalScraper(self.buoy_id, data_dir)

    def save_realtime(self, dtypes=None):
        '''
        Saves realtime data as pickled dataframes.
        Input :
            dtypes : Optional, list of data types to save. Default is all available data types.
        '''
        self.realtime.scrape_dtypes(dtypes)
    
    def save_historical(self, dtypes=None):
        '''
        Saves historical data as pickled dataframes.
        Input :
            dtypes : Optional, list of data types to save. Default is all available data types.
        '''
        self.historical.scrape_dtypes(dtypes)
    
    def get_realtime(self, dtype):
        '''
        Get realtime data (last 45 days) for specified data type
        Input :
            dtype : string representing data type to get data for.
        Output :
            pandas dataframe with datetime64[ns, UTC] index.
        '''
        if dtype not in self.realtime.DTYPES:
            print("Possible realtime dtypes are: {}".format(list(self.realtime.DTYPES)))
            return
        return self.realtime.scrape_dtype(dtype)
    
    def get_historical(self, dtype, year=None, month=None):
        '''
        Get realtime data (last 45 days) for specified data type
        Input :
            dtype : string representing data type to get data for.
            Up to one of the following (default is all available year and months):
                year : int, optional. Single year to get data for.
                month : int in range [1, 12], optional. Single month this year to get data for.
        Output :
            pandas dataframe with datetime64[ns, UTC] index.
        '''
        if dtype not in self.historical.DTYPES:
            print("Possible historical dtypes are: {}".format(list(self.historical.DTYPES)))
            return
        if year and month:
            raise Exception("Can only provide one of `year` and `month`.")
        if year:
            return self.historical.scrape_year(dtype, year)
        elif month:
            return self.historical.scrape_month(dtype, month)
        else:
            return self.historical.scrape_dtype(dtype)
    
    def load_realtime(self, dtype, timezone='UTC'):
        '''
        Loads dataframe that was previously saved with method `save_realtime`
        Input :
            dtype : string representing data type to load data for.
            timezone : string, optional. Timezone to set index to. Default is `UTC`
        Output :
            pandas dataframe
        '''
        path = "{}{}/realtime/{}.pkl".format(self.data_dir, self.buoy_id, dtype)
        return self._load_dataframe(path, timezone)

    def load_historical(self, dtype, timezone='UTC'):
        '''
        Loads dataframe that was previously saved with method `save_historical` 
        Input :
            dtype : string representing data type to load data for.
            timezone : string, optional. Timezone to set index to. Default is `UTC`
        Output :
            pandas dataframe
        '''
        path = "{}{}/historical/{}.pkl".format(self.data_dir, self.buoy_id, dtype)
        return self._load_dataframe(path, timezone)

    def _load_dataframe(self, file_path, timezone):
        ''' Helper method to load a pickled dataframe. '''
        try:
            data = pd.read_pickle(file_path)
            return data.tz_convert(timezone)
        except OSError:
            print("No pickle at {}".format(file_path))
        except:
            print("{} is not a valid timezone for pandas DataFrame.tz_convert() method.".format(timezone))


    def _populate_metadata(self, buoy_id):
        ''' Helper method to populate metadata field with relevant information. '''
        STATION_INFO_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
        OWNERS_URL = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"
        # Get all stations info
        stations = pd.read_csv(STATION_INFO_URL, delimiter = "|", index_col = 0).iloc[1:,:]
        # Check if buoy id is valid
        if str(buoy_id) not in stations.index:
            raise Exception("{} is not a valid buoy id.".format(buoy_id))
        stations.index.name = 'station_id'
        stations.columns = ['owner', 'ttype', 'hull', 'name', 'payload', 'location', 'timezone', 'forecast', 'note']
        self.metadata = {}
        self.metadata['station_id'] = buoy_id
        # Get owner's info to format with owner code from stations info
        try:
            owners = pd.read_csv(OWNERS_URL, delimiter="|", skiprows=1, index_col=0)
            owner = owners.loc["{:<3}".format(stations.loc[buoy_id,'owner']), :]
            self.metadata['owner'] = "{}, {}".format(owner[0].rstrip(), owner[1].rstrip())
        except:
            self.metadata['owner'] = 'NaN'
        self.metadata['ttype'] = stations.loc[buoy_id,'ttype']
        self.metadata['hull'] = stations.loc[buoy_id,'hull']
        self.metadata['name'] = stations.loc[buoy_id,'name']
        self.metadata['latitude'] = re.search('.{7}[NS]', stations.loc[buoy_id,'location']).group(0)
        self.metadata['longitude'] = re.search('.{7}[WE]', stations.loc[buoy_id,'location']).group(0)
        self.metadata['timezone'] = stations.loc[buoy_id,'timezone']
        self.metadata['forecast'] = stations.loc[buoy_id,'forecast']  # More Forecasts: https://www.ndbc.noaa.gov/data/DAB_Forecasts/46087fc.html, https://www.ndbc.noaa.gov/data/Forecasts/
        self.metadata['note'] = stations.loc[buoy_id,'note']
        # self.metadata['available historical'] = {"dtype":[years]}

    def __repr__(self):
        return "Station ID: {}\nStation Name: {}\nLocation: {}, {}\nTime Zone: {}\nOwner: {}\nTtype: {}\nNotes: {}".format(
            self.buoy_id, self.metadata['name'], self.metadata['latitude'], self.metadata['longitude'], self.metadata['timezone'],
            self.metadata['owner'], self.metadata['ttype'], self.metadata['note']
        )