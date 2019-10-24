"""
Jonah Golden, 2019
Class to scrape realtime data from NDBC Buoys -- https://www.ndbc.noaa.gov/realtime.shtml
Creates pandas dataframe objects for specific data types for the last 45 days.
Options to return or pickle dataframes.

Notes:
    * Realtime data contains some information that historical data doesnt,
        for example, Separation Frequencies in the data_spec dtype.
    * You can run 'scrape_all_dtypes' once every 45 days to keep accumulatine realtime data.
"""

import pandas as pd
from .buoy_data_scraper import BuoyDataScraper


class RealtimeScraper(BuoyDataScraper):

    DTYPES = {"stdmet":   {"url_code":"txt",       "name":"Standard Meteorological Data"},
              "adcp":     {"url_code":"adcp",      "name":"Acoustic Doppler Current Profiler Data"},
              "cwind":    {"url_code":"cwind",     "name":"Continuous Winds Data"},
              "supl":     {"url_code":"supl",      "name":"Supplemental Measurements Data"},
              "spec":     {"url_code":"spec",      "name":"Spectral Wave Summary Data"},
              "data_spec":{"url_code":"data_spec", "name":"Raw Spectral Wave Data"},
              "swdir":    {"url_code":"swdir",     "name":"Spectral Wave Data (alpha1)"},
              "swdir2":   {"url_code":"swdir2",    "name":"Spectral Wave Data (alpha2)"},
              "swr1":     {"url_code":"swr1",      "name":"Spectral Wave Data (r1)"},
              "swr2":     {"url_code":"swr2",      "name":"Spectral Wave Data (r2)"}
              }
    BASE_URL = "https://www.ndbc.noaa.gov/data/realtime2/{}.{}"

    def __init__(self, buoy_id, data_dir = "data/"):
        super().__init__(buoy_id)
        self.data_dir = "{}{}/realtime/".format(data_dir, buoy_id)

    def get_available_dtypes(self):
        '''
        Returns list of available realtime data types for this buoy.
        '''
        available_types = []
        for dtype in self.DTYPES:
            if self._url_valid(self._make_url(dtype)):
                available_types.append(dtype)
        return available_types

    def scrape_all_dtypes(self, data_dir=None):
        '''
        Scrapes and saves all data types for this buoy for realtime data (the last 45 days).
        Input :
            data_dir (optional) : string representing data directory to save dataframes to as pickles.
                                  if unspecified, will use classes data directory (default = "../data/").
        Output :
            saves all dtypes available for this buoy as pandas dataframe pickles.
            dtypes with previously saved pickles in data_dir will be updated with new data.
        '''
        if not data_dir: data_dir = self.data_dir
        self._create_dir_if_not_exists(data_dir)
        for dtype in self.get_available_dtypes():
            self.scrape_dtype(dtype, save=True, save_path="{}{}.pkl".format(data_dir, dtype))

    def scrape_dtype(self, dtype, save=False, save_path=None):
        '''
        Scrapes data for a given data type. Calls helper function to scrape specific dtype.
        See helper functions below for columns and units of each dtype.
        More info at: https://www.ndbc.noaa.gov/measdes.shtml
        Inputs :
            dtype : string, must be an available data type for this buoy.
            save_path (optional) : string representing path to save dataframe to as pickle. Will update pickle if it already exists.
        Output :
            pandas dataframe. If save_path is specified, also saves pickled dataframe to save_path.
        '''
        url = self._make_url(dtype)
        if self._url_valid(url):
            df = getattr(self, dtype)(url)
            if save:
                if not save_path:
                    self._create_dir_if_not_exists(self.data_dir)
                    save_path="{}{}.pkl".format(self.data_dir, dtype)
                self._update_pickle(df, save_path)
            else:
                return df
        else:
            print("{} not available for buoy {}. Use method 'get_available_dtypes' to see which data types are available for this buoy.".format(dtype, self.buoy_id))

    def stdmet(self, url):
        '''
        Standard Meteorological Data
        dtype:   "stdmet"
        index:   datetime64[ns, UTC]
        columns: WDIR  WSPD  GST  WVHT  DPD  APD  MWD  PRES  ATMP  WTMP  DEWP  VIS  PTDY  TIDE
        units:   degT  m/s   m/s   m    sec  sec  degT  hPa  degC  degC  degC  nmi  hPa    ft
        '''
        HEADERS, NA_VALS = [0,1], ['MM']
        return self._scrape_norm(url, HEADERS, NA_VALS)

    def adcp(self, url):
        '''
        Acoustic Doppler Current Profiler Data
        dtype:   "adcp"
        index:   datetime64[ns, UTC]
        columns: DEP01  DIR01  SPD01
        units:   m      degT   cm/s
        '''
        HEADERS, NA_VALS = [0,1], ['MM']
        df = self._scrape_norm(url, HEADERS, NA_VALS)
        return df.iloc[:,0:3].astype('float')

    def cwind(self, url):
        '''
        Continuous Winds Data
        dtype:   "cwind"
        index:   datetime64[ns, UTC]
        columns: WDIR  WSPD  GDR  GST  GTIME
        units:   degT  m/s   degT m/s  hhmm
        '''
        HEADERS, NA_VALS = [0,1], ['MM', 99.0, 999, 9999]
        return self._scrape_norm(url, HEADERS, NA_VALS).astype('float')

    def supl(self, url):
        '''
        Supplemental Measurements Data
        dtype:   "supl"
        index:   datetime64[ns, UTC]
        columns: PRES  PTIME  WSPD  WDIR  WTIME
        units:   hPa   hhmm   m/s   degT  hhmm
        '''
        HEADERS, NA_VALS = [0,1], ['MM']
        return self._scrape_norm(url, HEADERS, NA_VALS).astype('float')

    def spec(self, url):
        '''
        Spectral Wave Summary Data
        dtype:   "spec"
        index:   datetime64[ns, UTC]
        columns: WVHT  SwH  SwP  WWH  WWP  SwD  WWD  STEEPNESS  APD  MWD
        units:   m     m    sec   m   sec   -   degT     -      sec  degT
        '''
        HEADERS, NA_VALS = [0,1], ['MM', -99]
        return self._scrape_norm(url, HEADERS, NA_VALS)

    def data_spec(self, url):
        '''
        Raw Spectral Wave Data
        dtype:   "data_spec"
        index:   datetime64[ns, UTC]
        columns: sep_freq        0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   frequency (Hz)  Spectral Wave Density/Energy in m^2/Hz for each frequency bin
        Note: "sep_freq" (Separation Frequency) is the frequency that separates
                wind waves (WWH, WWP, WWD) from swell waves (SWH, SWP,SWD).
        '''
        NA_VALS = ['MM', 9.999]
        # combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(url, header=None, na_values=NA_VALS, delim_whitespace=True, skiprows=1, parse_dates={'datetime':[0,1,2,3,4]}, index_col=0)
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M").tz_localize('UTC')
        # separate headers and data
        # first column is the Separation Frequencies
        sep_freqs = df.iloc[:, 0:1]
        # rest of the columns alternate between [spectral density, (frequency of that density)]
        specs = sep_freqs.join(df.iloc[:, 1::2])
        freqs = df.iloc[0, 2::2]
        specs.columns = pd.concat([pd.Series(["sep_freq"]), freqs])
        # remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','') for cname in specs.columns]
        specs.columns.name = 'frequencies'
        return specs

    def swdir(self, url):
        '''
        Spectral Wave Data (alpha1, mean wave direction)
        dtype:   "swdir"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   direction (in degrees from true North, clockwise) for each frequency bin.
        '''
        NA_VALS = ['MM', 999.0]
        return self._scrape_spectral(url, NA_VALS)

    def swdir2(self, url):
        '''
        Spectral Wave Data (alpha2, principal wave direction)
        dtype:   "swdir2"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   direction (in degrees from true North, clockwise) for each frequency bin.
        '''
        NA_VALS = ['MM', 999.0]
        return self._scrape_spectral(url, NA_VALS)

    def swr1(self, url):
        '''
        Spectral Wave Data (r1, directional spreading for alpha1)
        dtype:   "swr1"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   Ratio (between 0 and 1) describing the spreading about the main direction.
        '''
        NA_VALS = ['MM', 999.0]
        return self._scrape_spectral(url, NA_VALS)

    def swr2(self, url):
        '''
        Spectral Wave Data (r2, directional spreading for alpha2)
        dtype:   "swr2"
        index:   datetime64[ns, UTC]
        columns: 0.033  0.038  0.043 ... 0.445	0.465	0.485 (frequencies in Hz)
        units:   Ratio (between 0 and 1) describing the spreading about the main direction.
        '''
        NA_VALS = ['MM', 999.0]
        return self._scrape_spectral(url, NA_VALS)

    def _scrape_spectral(self, url, na_vals):
        '''
        Scrapes data for "spectral" data types that don't have separation frequency column (swdir, swdir2, swr1, swr2).
        All of these data types share similar formats.
        Inputs :
            url : string
                the url to scrape data from
            na_vals : list
                values that should be treated as NA
        Output :
            df : pandas dataframe with datetime index localized to UTC representing the data
        '''
        # combine the first five date columns YY MM DD hh mm and make index
        df = pd.read_csv(url, header=None, na_values=na_vals, delim_whitespace=True, skiprows=1, parse_dates={'datetime':[0,1,2,3,4]}, index_col=0)
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M").tz_localize('UTC')
        # separate headers and data
        specs = df.iloc[:, 0::2]
        freqs = df.iloc[0, 1::2]
        specs.columns = freqs
        # remove the parenthesis from the column index
        specs.columns = [cname.replace('(','').replace(')','') for cname in specs.columns]
        specs.columns.name = 'frequencies'
        return specs

    def _make_url(self, dtype):
        return self.BASE_URL.format(self.buoy_id, self.DTYPES[dtype]["url_code"])

    def _update_pickle(self, df, path):
        # If pickle for this dtype exists, add to it
        try:
            old_df = pd.read_pickle(path)
            last_update_time = max(old_df.index)
            new_items = df[df.index > last_update_time]
            new_df = pd.concat([old_df, new_items]).sort_index()
            new_df.to_pickle(path)
            print("Added {} new items to {}".format(len(new_items), path))
        # Otherwise, create it
        except (OSError, IOError):
            df.to_pickle(path)
            print("Saved data to {}".format(path))
