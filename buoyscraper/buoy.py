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
import re   # For metadata parsing
from .datascrapers import RealtimeScraper
from .datascrapers import HistoricalScraper

class Buoy:

    def __init__(self, buoy_id, data_dir="buoydata/"):
        self.buoy_id = str(buoy_id)
        self.metadata = self._get_metadata()
        self.data_dir = data_dir
        self.realtime = RealtimeScraper(self.buoy_id, data_dir)
        self.historical = HistoricalScraper(self.buoy_id, data_dir)

    @staticmethod
    def get_buoys():
        '''Get all available buoys. Returns pandas dataframe with buoy ids as index.'''
        STATIONS_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
        # Get and format all stations info
        stations = pd.read_csv(STATIONS_URL, delimiter = "|", index_col = 0).iloc[1:,:]
        stations.index.name = 'station_id'
        stations.columns = ['owner', 'ttype', 'hull', 'name', 'payload', 'location', 'timezone', 'forecast', 'note']
        return stations

    ### On Init methods

    def _get_metadata(self):
        ''' Helper method to populate metadata field with relevant information. '''
        stations = self.get_buoys()
        # Check if buoy id is valid
        if self.buoy_id not in stations.index:
            raise ValueError("{} is not a valid buoy id. Use static method Buoy.get_buoys() to get dataframe of all buoys.".format(self.buoy_id))
        
        # Populate metadata
        buoy_info = stations.loc[self.buoy_id,:]
        metadata = {}
        metadata['buoy_id'] = self.buoy_id
        metadata['owner'] = self._get_owner_name(buoy_info['owner'])
        metadata['ttype'] = buoy_info['ttype']
        metadata['hull'] = buoy_info['hull']
        metadata['name'] = buoy_info['name']
        metadata['timezone'] = buoy_info['timezone']
        metadata['forecast'] = buoy_info['forecast']  # More Forecasts: https://www.ndbc.noaa.gov/data/DAB_Forecasts/46087fc.html, https://www.ndbc.noaa.gov/data/Forecasts/
        metadata['note'] = buoy_info['note']
        # metadata['available historical'] = {"dtype":[years]}

        # Latitude and Longitude parsing
        lat_match = re.search(r'([0-9]{1,3}\.[0-9]{3}) ([NS])', buoy_info['location'])
        lat = lat_match.group(1)
        if lat_match.group(2) == 'S':
            lat = '-' + lat
        metadata['latitude'] = lat
        lng_match = re.search(r'([0-9]{1,3}\.[0-9]{3}) ([WE])', buoy_info['location'])
        lng = lng_match.group(1)
        if lng_match.group(2) == 'W':
            lng = '-' + lng
        metadata['longitude'] = lng

        return metadata

    def _get_owner_name(self, owner_code):
        ''' Metadata helper function gets a buoy owner's full name based on buoy owner code. '''
        OWNERS_URL = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"
        try:
            owners = pd.read_csv(OWNERS_URL, delimiter="|", skiprows=1, index_col=0)
            owner = owners.loc["{:<3}".format(owner_code), :]
            return "{}, {}".format(owner[0].rstrip(), owner[1].rstrip())
        except:
            return 'NaN'

    ### Getting Data methods

    def get_realtime_dtypes(self):
        '''Returns list of available realtime data types for this buoy.'''
        return self.realtime.get_available_dtypes()

    def get_historical_dtypes(self, dtypes=[], years=[], months=[]):
        '''
        Returns dict of available historical data types for this buoy based on inputs.
        Note: Depending on inputs, this method is quite slow. TODO Make it faster.
        Inputs :
            dtypes : Optional. List of dtype strings to get available months and years for. Default is all dtypes.
            years : Optional. List of year ints to get available dtypes for.
            months : Optional. List of month ints to get available dtypes for.
        Output :
            dictionary representing available historical data based on inputs.
        '''
        available = {}
        # If no inputs are provided, get all available data types.
        if len(dtypes) == 0 and len(years) == 0 and len(months) == 0:
            dtypes = self.historical.DTYPES
        for dtype in dtypes:
            available[dtype] = {}
            available[dtype]['months']=self.historical._available_months(dtype)
            available[dtype]['years']=self.historical._available_years(dtype)
        if len(years) > 0:
            available['years'] = {}
            for year in years:
                available['years'][year] = self.historical._available_dtypes_year(year)
        if len(months) > 0:
            available['months'] = {}
            for month in months:
                available['months'][month] = self.historical._available_dtypes_month(month)
        return available

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
        else: 
            df = self.realtime.scrape_dtype(dtype)
            if df.empty:
                print("{} not available for buoy {}. Use method 'get_realtime_dtypes' to get available realtime data types for this buoy.".format(dtype, self.buoy_id))
            else: return df
    
    def get_historical(self, dtype, year=None, month=None):
        '''
        Get realtime data (last 45 days) for specified data type
        Input :
            dtype : string representing data type to get data for.
            Up to one of the following (default is all available data):
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
            df = self.historical.scrape_year(dtype, year)
            if df.empty: print("{} for year {} not available for buoy {}. Use method 'get_historical_dtypes(year={})' to get available historical data types for this buoy and year.".format(dtype,year,self.buoy_id,year))
            else: return df
        elif month:
            df = self.historical.scrape_month(dtype, month)
            if df.empty: print("{} for month {} not available for buoy {}. Use method 'get_historical_dtypes(month={})' to get available historical data types for this buoy and month.".format(dtype,month,self.buoy_id,month))
            else: return df
        else:
            df = self.historical.scrape_dtype(dtype)
            if df.empty: print("{} not available for buoy {}. Use method 'get_historical_dtypes()' to get available historical data types for this buoy.".format(dtype,self.buoy_id))
            else: return df

    ### Saving / Loading methods

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
    
    def load_data(self, dtype, cols=None, timezone='UTC'):
        real, hist = self.load_realtime(dtype, timezone), self.load_historical(dtype, timezone)
        both = pd.concat([hist, real], sort=True)
        if cols: return both[cols]
        else: return both

    def _load_dataframe(self, file_path, timezone):
        ''' Helper method to load a pickled dataframe. '''
        try:
            data = pd.read_pickle(file_path)
            return data.tz_convert(timezone)
        except OSError:
            print("No pickle at {}".format(file_path))
        except:
            print("{} is not a valid timezone for pandas DataFrame.tz_convert() method.".format(timezone))

    def __repr__(self):
        return "Station ID: {}\nStation Name: {}\nLocation: {}, {}\nTime Zone: {}\nOwner: {}\nTtype: {}\nNotes: {}".format(
            self.buoy_id, self.metadata['name'], self.metadata['latitude'], self.metadata['longitude'], self.metadata['timezone'],
            self.metadata['owner'], self.metadata['ttype'], self.metadata['note']
        )