import pandas as pd
from datetime import datetime
import requests  # For checking url validity
import os  # For saving to data directory


class Buoy:
    SPECTRAL_COLUMNS = ['.0200', '.0325', '.0375', '.0425', '.0475', '.0525', '.0575', '.0625', '.0675', '.0725', '.0775', '.0825', '.0875', '.0925', '.1000', '.1100', '.1200', '.1300', '.1400', '.1500', '.1600', '.1700', '.1800', '.1900', '.2000', '.2100', '.2200', '.2300', '.2400', '.2500', '.2600', '.2700', '.2800', '.2900', '.3000', '.3100', '.3200', '.3300', '.3400', '.3500', '.3650', '.3850', '.4050', '.4250', '.4450', '.4650', '.4850']
    URL_CODES = {"stdmet": {"name":"Standard metorological", "code":"h", "col_names":["wind_direction", "wind_speed", "gust", "wave_height", "dominant_period", "average_period", "wave_direction", "pressure", "air_temp", "water_temp", "dew_point", "vis", "tide"]},
                 "swden": {"name":"Spectral wave density", "code":"w", "col_names":SPECTRAL_COLUMNS},
                 "swdir": {"name":"Spectral wave (alpha1) direction", "code":"d", "col_names":SPECTRAL_COLUMNS},
                 "swdir2": {"name":"Spectral wave (alpha2) direction", "code":"i", "col_names":SPECTRAL_COLUMNS},
                 "swr1": {"name":"Spectral wave (r1) direction", "code":"j", "col_names":SPECTRAL_COLUMNS},
                 "swr2": {"name":"Spectral wave (r2) direction", "code":"k", "col_names":SPECTRAL_COLUMNS},
                 "adcp": {"name":"Ocean current", "code":"a", "col_names":["current_depth", "current_direction", "current_speed"]}}
                 #"cwind": {"name":"Continuous winds", "code":"c"}}
    MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    YEARS = [year for year in range(2007, 2019)]
    MONTH_URL = "https://www.ndbc.noaa.gov/view_text_file.php?filename={}{}{}.txt.gz&dir=data/{}/{}/"
    YEAR_URL = "https://www.ndbc.noaa.gov/view_text_file.php?filename={}{}{}.txt.gz&dir=data/historical/{}/"


    def __init__(self, station_id, latitude="", longitude=""):
            self.id = station_id
            self.lat = latitude
            self.lon = longitude
            self.data_dir = "../data/{}/".format(station_id)

    def get_historical_data(self, save=False, filename_prefix="", verbose=True):
        if save: self._check_directory()
        else: all_data = {}
        for dtype in self.URL_CODES:
            months = self.get_data_months(dtype)
            years = self.get_data_years(dtype)
            both = months.append(years)
            if save:
                both.to_pickle("{}{}{}.pkl".format(self.data_dir, filename_prefix, dtype))
                if verbose: print("Saved", dtype, "data", both.shape, sep=" ")
            else:
                all_data[dtype] = both
                if verbose: print("Retrieved", dtype, "data", both.shape, sep=" ")
        if save: print("Data saved to", self.data_dir, sep=" ")
        else: return all_data

    def get_data_months(self, dtype, month_nums=range(1, len(MONTHS)+1)):
        data = pd.DataFrame(columns = self.URL_CODES[dtype]["col_names"])
        for month in month_nums:
            next_month = self.get_month_data(dtype, month)
            if next_month:
                data = data.append(next_month)
        return data

    def get_month_data(self, dtype, month_num):
        url = self._make_url_month(dtype, month_num)
        return self._get_url_data(url, dtype)

    def get_data_years(self, dtype, years=YEARS):
        data = pd.DataFrame(columns = self.URL_CODES[dtype]["col_names"])
        for year in years:
            next_year = self.get_year_data(dtype, year)
            if next_year:
                data = data.append(next_year)
        return data

    def get_year_data(self, dtype, year):
        url = self._make_url_year(dtype, year)
        return self._get_url_data(url, dtype)

    def _get_url_data(self, url, dtype=None, print_err=False):
        if self._url_not_valid(url):
            if print_err: print("Invalid URL: {}".format(url))
            return False
        data = self._read_url_data(url).sort_index()
        return self._format_data(dtype, data) if dtype else data

    def _format_data(self, dtype, data):
        if dtype == "adcp":  # TODO: clean this up
            data = data.iloc[:,0:3]
        data.columns = self.URL_CODES[dtype]["col_names"]
        return data

    def _read_url_data(self, url, header=[0,1], delim = r"\s+"):
        return pd.read_csv(url, header=header, delimiter=delim,
                           parse_dates={'time':[0,1,2,3,4]}, index_col=0, date_parser=self._dateparse)

    def _dateparse(self, x):
        '''Helper for read_url_data'''
        return pd.datetime.strptime(x, '%Y %m %d %H %M')

    def _make_url_year(self, dtype, year):
        return self.YEAR_URL.format(self.id, self.URL_CODES[dtype]["code"], year, dtype)
        # return ("https://www.ndbc.noaa.gov/view_text_file.php?filename=" + self.id + self.URL_CODES[dtype]["code"] + str(year) + ".txt.gz&dir=data/historical/" + dtype + "/")
    
    def _make_url_month(self, dtype, month_num):
        return self.MONTH_URL.format(self.id, month_num, datetime.now().year, dtype, self.MONTHS[month_num-1])
        # return ("https://www.ndbc.noaa.gov/view_text_file.php?filename=" + self.id + str(month_id) + str(year) + ".txt.gz&dir=data/" + dtype + "/" + month_name + "/")

    def _url_not_valid(self, url):
        request = requests.get(url)
        return request.status_code != 200

    def _check_directory(self):
        try:
            os.makedirs(self.data_dir)
        except FileExistsError:
            pass


    

    