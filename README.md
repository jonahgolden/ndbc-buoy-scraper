# buoyscraper

`buoyscraper` is a Python package for scraping [NDBC buoy data](https://www.ndbc.noaa.gov/).

* [Realtime Data](#Realtime-Data)
* [Historical Data](#Historical-Data)
* [Metadata](#Metadata)
* [Contribute](#Contribute)

## Use
---
All data can be accessed using the Buoy class. A Buoy object takes an [NDBC buoy id](https://www.ndbc.noaa.gov/) to initialize, and optionally, a directory to save data to (default is "buoydata/"). Buoy objects contain metadata about the buoy and methods for retrieving and saving historical and realtime data from the specified buoy.

### Realtime Data
Realtime data is available for the most recent 45 days. Currently available **realtime dtypes are:**

- "stdmet" (Standard Meteorological Data)
- "adcp" (Acoustic Doppler Current Profiler Data)
- "cwind" (Continuous Winds Data)
- "supl" (Supplemental Measurements Data)
- "spec" (Spectral Wave Summary Data)
- "data_spec" (Raw Spectral Wave Data)
- "swdir" (Spectral Wave Data alpha1)
- "swdir2" (Spectral Wave Data alpha2)
- "swr1" (Spectral Wave Data r1)
- "swr2" (Spectral Wave Data r2)

Realtime data for a specific data type is accessed by calling the `get_realtime` method on a Buoy object, which takes a data type as a string input and returns a pandas dataframe. For example:
```python
>>> from buoyscraper import Buoy
>>> neah_buoy = Buoy(46087)
>>> neah_stdmet = neah_buoy.get_realtime('stdmet')
```

All realtime data can be saved by calling the `save_realtime` method on a Buoy object and not specifying any data types.  Alternatively, a list of data type strings can be specified.  Saved realtime data can be loaded by calling the `load_realtime` method on a Buoy object, which takes a data type as input, and returns a pandas dataframe. For example:
```python
>>> from buoyscraper import Buoy
>>> neah_buoy = Buoy(46087)
>>> neah_buoy.save_realtime()
>>> neah_swdir = neah_buoy.load_realtime('swdir')
```
*Note*: For realtime data only, if a data type has been previously saved in the default directory for a specific buoy, it will be updated with any new data if the `save_realtime` method is called again. This way, you can repeatedly update your realtime data as some information is only available for 45 days as realtime data (e.g. the Separation Frequency column in `dtype` 'data_spec').

### Historical Data
Historical data is currently available from 2007 through one month ago (sometimes not immediately). Currently available **historical dtypes are:**

- "stdmet" (Standard Meteorological Data)
- "adcp" (Acoustic Doppler Current Profiler Data)
- "cwind" (Continuous Winds Data)
- "swden" (Spectral wave density)
- "swdir" (Spectral Wave Data alpha1)
- "swdir2" (Spectral Wave Data alpha2)
- "swr1" (Spectral Wave Data r1)
- "swr2" (Spectral Wave Data r2)

Historical data for a specific data type is accessed by calling the `get_historical` method on a Buoy object, which takes a data type as a string input and returns a pandas dataframe. For example:
```python
>>> from buoyscraper import Buoy
>>> neah_buoy = Buoy(46087)
>>> neah_stdmet = neah_buoy.get_historical('stdmet')
```

All historical data can be saved by calling the `save_historical` method on a buoy object and not specifying any data types.  Alternatively, a list of data type strings can be specified.  Saved realtime data can be loaded by calling the `load_realtime` method on a Buoy object, which takes a data type as input, and returns a pandas dataframe. For example:
```python
>>> from buoyscraper import Buoy
>>> neah_buoy = Buoy(46087)
>>> neah_buoy.save_historical()
>>> neah_swden = neah_buoy.load_realtime('swden')
```

### Metadata
All supported metadata is generated as a dictionary when an object is initialized. Metadata can be accessed through the metadata field or by printing a Buoy object. For example:
```python
>>> from buoyscraper import Buoy
>>> neah_buoy = Buoy(46087)
>>> print(neah_buoy)
Station ID: 46087
Station Name: Neah Bay - 6 NM North of Cape Flattery, WA     (Traffic Separation Lighted Buoy)
Location: 48.493 N, 24.726 W
Time Zone: P
Owner: NDBC, US
Ttype: 3-meter discus buoy
Notes: nan
```

---
## Contribute
### To Do
* Add functionality for scraping Canadian buoys, eh?
* Incorporate "derived2" datatype into realtime scraper
* Tweak historical scraping functions to allow data scraping pre-2007, when formats were slightly differtent.
* Add available data types for buoys to their metadata:
    1. Realtime data types.
    2. Historical data types with available years for each.

Please submit a pull request if you would like to add addtional functionality!

-Jonah