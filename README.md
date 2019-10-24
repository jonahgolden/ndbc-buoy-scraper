# buoyscraper

`buoyscraper` is a Python wrapper for scraping NDBC [data](https://www.ndbc.noaa.gov/).

## Use
---
All data can be accessed using the RealtimeScraper and HistoricalScraper classes.  Both classes take a buoy id to initialize, contain metadata about the buoy, and have similar methods for retrieving data, and 

### Metadata
All supported metadata is generated as a dictionary when an object is initialized.
Metadata can be accessed through the metadata field, or simply by printing any RealtimeScraper or HistoricalScraper object.

### Realtime Data
Both `RealtimeScraper` and `HistoricalScraper` objects are initialized with a buoy_id, and optionally, a default directory to save data to.
All available buoys can be found through [NDBC](https://www.ndbc.noaa.gov/).

Data for a specific data type can be accessed by providing a `dtype` string to the `.scrape_dtype` method on a `RealtimeScraper` or `HistoricalScraper` object.

**Realtime** data is available for the last 45 days. Currently available realtime dtypes are:

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

**Historical** data is currently available from 2007 through one month ago (sometimes not immediately). Currently available historical dtypes are:

- "stdmet" (Standard Meteorological Data)
- "adcp" (Acoustic Doppler Current Profiler Data)
- "cwind" (Continuous Winds Data)
- "swden" (Spectral wave density)
- "swdir" (Spectral Wave Data alpha1)
- "swdir2" (Spectral Wave Data alpha2)
- "swr1" (Spectral Wave Data r1)
- "swr2" (Spectral Wave Data r2)

Data is returned as a pandas dataframe by default, or can optionally be saved as a pickled dataframe for later use.

*Note*: For realtime data only, if a pickle has been previously saved in the default directory for a specific buoy and data type, it will be updated with any new data if the `.scrape_dtype` method is called again.

Please submit a pull request if you would like to add addtional functionality!

-Jonah