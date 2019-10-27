"""
Jonah Golden, 2019
BuoyDataScraper class that contains common functionalities
for RealtimeScraper and HistoricalScraper.
"""

import pandas as pd
import requests # For checking url validity
import os       # For saving to data directory

class BuoyDataScraper:

    def __init__(self, buoy_id):
        self.buoy_id = buoy_id

    def _scrape_norm(self, url, headers=[0,1], na_vals=['MM'], date_cols=[0,1,2,3,4], date_format="%Y %m %d %H %M"):
        '''
        Scrapes data for "normal" data types (all historical after 2006, and realtime dtypes "stdmet", "adcp", "cwind", "supl", "spec").
        All of these data types share similar formats, with only differences being the header and NA values.
        Inputs :
            url : string
                the url to scrape data from
            header : list of ints
                the row number for the headers
            na_vals : list
                values that should be treated as NA
        Output :
            df : pandas dataframe with datetime index localized to UTC representing the data
        '''
        # read the data and combine first 5 columns into datetime index
        df = pd.read_csv(url, header=headers, delim_whitespace=True, na_values=na_vals, parse_dates={'datetime':date_cols}, index_col=0)
        df.index = pd.to_datetime(df.index,format=date_format).tz_localize('UTC')
        # remove all headers but first
        while df.columns.nlevels > 1:
            df.columns = df.columns.droplevel(1)
        df.columns.name = 'columns'
        return df

    def _url_valid(self, url):
        request = requests.get(url)
        return request.status_code == 200

    def _create_dir_if_not_exists(self, data_dir):
        try: os.makedirs(data_dir)
        except FileExistsError: pass