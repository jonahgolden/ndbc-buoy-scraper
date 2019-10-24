"""
Jonah Golden, 2019
BuoyDataScraper class that contains common functionalities
for RealtimeScraper and HistoricalScraper, both of which
can be used to scrape NDBC buoy data -- https://www.ndbc.noaa.gov/
"""

import pandas as pd
import re       # For gettin buoy info
import requests # For checking url validity
import os       # For saving to data directory

class BuoyDataScraper:

    def __init__(self, buoy_id):
        self.buoy_id = buoy_id
        self.metadata = {}
        self._populate_metadata(buoy_id)

    def _scrape_norm(self, url, headers, na_vals):
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
        df = pd.read_csv(url, header=headers, delim_whitespace=True, na_values=na_vals, parse_dates={'datetime':[0,1,2,3,4]}, index_col=0)
        df.index = pd.to_datetime(df.index,format="%Y %m %d %H %M").tz_localize('UTC')
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

    def _populate_metadata(self, buoy_id):
        '''
        Gets pertinent information about this buoy station, and populates metadata field.
        '''
        STATION_INFO_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
        OWNERS_URL = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"
        # Get and format all stations info
        stations = pd.read_csv(STATION_INFO_URL, delimiter = "|", index_col = 0).iloc[1:,:]
        stations.index.name = 'station_id'
        stations.columns = ['owner', 'ttype', 'hull', 'name', 'payload', 'location', 'timezone', 'forecast', 'note']
        # Get owners info to format with owner code from stations info
        try:
            owners = pd.read_csv(OWNERS_URL, delimiter="|", skiprows=1, index_col=0)
            owner = owners.loc["{:<3}".format(stations.loc[buoy_id,'owner']), :]
            self.metadata['owner'] = "{}, {}".format(owner[0].rstrip(), owner[1].rstrip())
        except:
            self.metadata['owner'] = 'NaN'
        self.metadata['ttype'] = stations.loc[buoy_id,'ttype']
        self.metadata['name'] = stations.loc[buoy_id,'name']
        self.metadata['latitude'] = re.search('.{7}[NS]', stations.loc[buoy_id,'location']).group(0)
        self.metadata['longitude'] = re.search('.{7}[WE]', stations.loc[buoy_id,'location']).group(0)
        self.metadata['timezone'] = stations.loc[buoy_id,'timezone']
        self.metadata['notes'] = stations.loc[buoy_id,'note']

    def __repr__(self):
        return "Station ID: {}\nStation Name: {}\nLocation: {}, {}\nTime Zone: {}\nOwner: {}\nTtype: {}\nNotes: {}".format(
            self.buoy_id, self.metadata['name'], self.metadata['latitude'], self.metadata['longitude'], self.metadata['timezone'],
            self.metadata['owner'], self.metadata['ttype'], self.metadata['notes']
        )