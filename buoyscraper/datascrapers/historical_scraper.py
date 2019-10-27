"""
Jonah Golden, 2019
Class to scrape historical data from NDBC Buoys -- https://www.ndbc.noaa.gov/historical_data.shtml
Creates pandas dataframe objects for specific data types and time periods.
Options to return or pickle dataframes.

Notes:
    * Current implementation scrapes data from 2007 through most recent month.
        Data before 2007 was formatted slightly differently, and requires some tweaking of functions.
"""

import pandas as pd
from datetime import datetime
from .buoy_data_scraper import BuoyDataScraper

class HistoricalScraper(BuoyDataScraper):

    DTYPES = {"stdmet":{"url_code":"h", "name":"Standard metorological"},
              "swden": {"url_code":"w", "name":"Spectral wave density"},
              "swdir": {"url_code":"d", "name":"Spectral wave (alpha1) direction"},
              "swdir2":{"url_code":"i", "name":"Spectral wave (alpha2) direction"},
              "swr1":  {"url_code":"j", "name":"Spectral wave (r1) direction"},
              "swr2":  {"url_code":"k", "name":"Spectral wave (r2) direction"},
              "adcp":  {"url_code":"a", "name":"Ocean current"},
              "cwind": {"url_code":"c", "name":"Continuous winds"},
              "ocean": {"url_code":"o", "name":"Oceanographic"},
              "dart":  {"url_code":"t", "name":"Water column height (Tsunami) (DART)"}
              }
    BASE_URL_YEAR = "https://www.ndbc.noaa.gov/view_text_file.php?filename={}{}{}.txt.gz&dir=data/historical/{}/"
    BASE_URL_MONTH = "https://www.ndbc.noaa.gov/view_text_file.php?filename={}{}{}.txt.gz&dir=data/{}/{}/"
    MONTHS = {1: {"name":"Jan", "url_code":1},
              2: {"name":"Feb", "url_code":2},
              3: {"name":"Mar", "url_code":3},
              4: {"name":"Apr", "url_code":4},
              5: {"name":"May", "url_code":5},
              6: {"name":"Jun", "url_code":6},
              7: {"name":"Jul", "url_code":7},
              8: {"name":"Aug", "url_code":8},
              9: {"name":"Sep", "url_code":9},
              10:{"name":"Oct", "url_code":'a'},
              11:{"name":"Nov", "url_code":'b'},
              12:{"name":"Dec", "url_code":'c'}
              }
    MIN_YEAR = 2007

    def __init__(self, buoy_id, data_dir="buoydata/"):
            super().__init__(buoy_id)
            self.data_dir = "{}{}/historical/".format(data_dir, buoy_id)

    def scrape_dtypes(self, dtypes=None):
        '''
        Scrapes and saves all known historical data for this buoy.
        Input :
            dtypes : Optional, list of dtype strings. Default is all available dtypes.
        Notes : * If self.data_dir doesn't exist, it will be created.
                * Existing files will be overwritten.
        '''
        if not dtypes: dtypes=self.DTYPES
        for dtype in dtypes:
            self.scrape_dtype(dtype, save=True)

    def scrape_dtype(self, dtype, save=False):
        '''
        Scrapes and optionally saves all historical data for a given dtype.
        Input :
            dtype : string, must be an available data type for this buoy
            save_pkl : default False. If True, saves data frame as pickle.
            data_dir : default self.data_dir.  directory to save data to is save_pkl is True.
        Output :
            pandas dataframe. If save_pkl is True, also saves pickled dataframe.
        Notes : * If self.data_dir doesn't exist, it will be created.
                * If save_pkl is True, existing file will be overwritten.
        '''
        df = pd.DataFrame()
        for year in range(self.MIN_YEAR, datetime.now().year):
            data = self.scrape_year(dtype, year)
            if not data.empty:
                if df.empty: df = data
                else: df = df.append(data)
        for month in range(1, datetime.now().month):
            data = self.scrape_month(dtype, month)
            if not data.empty:
                if df.empty: df = data
                else: df = df.append(data)
        if not df.empty and save:
            self._create_dir_if_not_exists(self.data_dir)
            path = "{}{}.pkl".format(self.data_dir, dtype)
            df.to_pickle(path)
            print("Saved data to {}".format(path))
        else:
            return df

    def scrape_year(self, dtype, year):
        '''
        Scrapes data for a given dtype and year. Calls helper function to scrape specific dtype.
        See helper functions below for columns and units of each dtype.
        More info at: https://www.ndbc.noaa.gov/measdes.shtml
        Input :
            dtype : string, must be an available data type for this buoy
            year : int in range 2006 and this year, not inclusive.
        Output :
            pandas dataframe.
        '''
        if year < self.MIN_YEAR:
            raise AttributeError("Minimum year is {}".format(self.MIN_YEAR))
        url = self._make_url_year(dtype, year)
        df = pd.DataFrame()
        if self._url_valid(url):
            df = getattr(self, dtype)(url)
        return df

    def scrape_month(self, dtype, month):
        '''
        Scrapes data for a given dtype and month. Calls helper function to scrape specific dtype.
        See helper functions below for columns and units of each dtype.
        More info at: https://www.ndbc.noaa.gov/measdes.shtml
        Input :
            dtype : string, must be an available data type for this buoy
            month : int in range 0 and this month, not inclusive.
        Output :
            pandas dataframe.
        Note: Data for most recent month may not yet be available.
        '''
        url = self._make_url_month(dtype, month)
        df = pd.DataFrame()
        if self._url_valid(url):
            df = getattr(self, dtype)(url)
        return df

    def stdmet(self, url):
        '''
        Standard Meteorological Data
        dtype:   "stdmet"
        index:   datetime64[ns, UTC]
        columns: WDIR  WSPD  GST  WVHT  DPD  APD  MWD  PRES  ATMP  WTMP  DEWP  VIS  PTDY  TIDE
        units:   degT  m/s   m/s   m    sec  sec  degT  hPa  degC  degC  degC  nmi  hPa    ft
        '''
        NA_VALS = ['MM', 99., 999.]
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'columns'
        return df

    def swden(self, url):
        '''
        Spectral wave density
        dtype:   "swden"
        index:   datetime64[ns, UTC]
        columns: .0200  .0325  .0375  ...  .4450  .4650  .4850 (frequencies in Hz)
        units:   Spectral Wave Density/Energy in m^2/Hz for each frequency bin
        '''
        NA_VALS = ['MM']
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'frequencies'
        return df

    def swdir(self, url):
        '''
        Spectral Wave Data (alpha1, mean wave direction)
        dtype:   "swdir"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   direction (in degrees from true North, clockwise) for each frequency bin.
        '''
        NA_VALS = ['MM', 999.]
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'frequencies'
        return df.astype('float')

    def swdir2(self, url):
        '''
        Spectral Wave Data (alpha2, principal wave direction)
        dtype:   "swdir2"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   direction (in degrees from true North, clockwise) for each frequency bin.
        '''
        NA_VALS= ['MM', 999.]
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'frequencies'
        return df.astype('float')

    def scrape_swr1(self, url):
        '''
        Spectral Wave Data (r1, directional spreading for alpha1)
        dtype:   "swr1"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   Ratio (between 0 and 100) describing the spreading about the main direction.
        Note:    r1 and r2 historical values are scaled by 100.
                 Units are hundredths, so they are multiplied by 0.01 here.
        '''
        NA_VALS, FACTOR = ['MM', 999.], 0.01
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'frequencies'
        df[df.select_dtypes(include=['number']).columns] *= FACTOR
        return df

    def swr2(self, url):
        '''
        Spectral Wave Data (r2, directional spreading for alpha2)
        dtype:   "swr2"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   Ratio (between 0 and 100) describing the spreading about the main direction.
        Note:    r1 and r2 historical values are scaled by 100.
                 Units are hundredths, so they are multiplied by 0.01 here.
        '''
        NA_VALS, FACTOR = ['MM', 999.], 0.01
        df = self._scrape_norm(url, na_vals=NA_VALS)
        df.columns.name = 'frequencies'
        df[df.select_dtypes(include=['number']).columns] *= FACTOR
        return df

    def adcp(self, url):
        '''
        Acoustic Doppler Current Profiler Data
        dtype:   "adcp"
        index:   datetime64[ns, UTC]
        columns: DEP01  DIR01  SPD01
        units:   m      degT   cm/s
        '''
        NA_VALS = ['MM']
        df = self._scrape_norm(url, na_vals=NA_VALS)
        return df.iloc[:,0:3].astype('float')

    def cwind(self, url):
        '''
        Continuous Winds Data
        dtype:   "cwind"
        index:   datetime64[ns, UTC]
        columns: WDIR  WSPD  GDR  GST  GTIME
        units:   degT  m/s   degT m/s  hhmm
        '''
        NA_VALS = ['MM', 99., 999., 9999.]
        df = self._scrape_norm(url, na_vals=NA_VALS)
        return df

    def ocean(self, url):
        '''
        Oceanographic Data
        dtype:   "ocean"
        index:   datetime64[ns, UTC]
        columns: DEPTH  OTMP  COND   SAL  O2%  O2PPM  CLCON  TURB  PH  EH
        units:   m      degC  mS/cm  psu  %    ppm    ug/l   FTU   -   mv
        '''
        NA_VALS = ['MM', 99., 999.]
        return self._scrape_norm(url, na_vals=NA_VALS)

    def dart(self, url):
        '''
        Water column height (Tsunami) (DART)
        dtype:   "dart"
        index:   datetime64[ns, UTC]
        columns: T                           HEIGHT
        units:   enum (measurement type)     m (height of water column)
                 * 1 = 15-minute
                 * 2 = 1-minute
                 * 3 = 15-second
        Notes : * See Tsunami detection algorithm here: https://www.ndbc.noaa.gov/dart/algorithm.shtml  
        '''
        NA_VALS, DATE_COLS, DATE_FORMAT = ['MM', 9999.], [0,1,2,3,4,5], "%Y %m %d %H %M %S"
        return self._scrape_norm(url, na_vals=NA_VALS, date_cols=DATE_COLS, date_format=DATE_FORMAT)

    def _available_dtypes_year(self, year):
        '''Returns list of available data types for a given year.'''
        available_types = []
        for dtype in self.DTYPES:
            if self._url_valid(self._make_url_year(dtype, year)):
                available_types.append(dtype)
        return available_types

    def _available_dtypes_month(self, month):
        '''Returns list of available data types for a given month.'''
        available_types = []
        for dtype in self.DTYPES:
            if self._url_valid(self._make_url_month(dtype, month)):
                available_types.append(dtype)
        return available_types

    def _available_years(self, dtype):
        '''Returns list of available years for a given data type.'''
        available_years = []
        for year in range(self.MIN_YEAR, datetime.now().year):
            if self._url_valid(self._make_url_year(dtype, year)):
                available_years.append(year)
        return available_years

    def _available_months(self, dtype):
        '''Returns list of available months for a given data type.'''
        available_months = []
        for month in range(1, datetime.now().month):
            if self._url_valid(self._make_url_month(dtype, month)):
                available_months.append(month)
        return available_months

    def _make_url_year(self, dtype, year):
        '''Makes a url for a given data type and year.'''
        return self.BASE_URL_YEAR.format(self.buoy_id, self.DTYPES[dtype]["url_code"], year, dtype)

    def _make_url_month(self, dtype, month):
        '''Makes a url for a given data type and month.'''
        return self.BASE_URL_MONTH.format(self.buoy_id, self.MONTHS[month]["url_code"], datetime.now().year, dtype, self.MONTHS[month]["name"])