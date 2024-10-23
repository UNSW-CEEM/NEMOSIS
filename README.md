# NEMOSIS

A Python package for downloading historical data published by the Australian Energy Market Operator (AEMO)

-----

## Table of Contents

- [Download Windows Application (GUI)](#download-windows-application-gui)
- [Documentation](#documentation)
- [Support NEMOSIS](#support-nemosis)
- [Get Updates, Ask Questions](#get-updates-ask-questions)
- [Using the Python Interface (API)](#using-the-python-interface-api)
  - [Installing NEMOSIS](#installing-nemosis)
  - [Data from dynamic tables](#data-from-dynamic-tables)
    - [Workflows](#workflows)
      - [Dynamic data compiler](#dynamic-data-compiler)
        - [Filter options](#filter-options)
        - [Caching options](#caching-options)
      - [Cache compiler](#cache-compiler)
      - [Additional columns](#accessing-additional-table-columns)
  - [Data from static tables](#data-from-static-tables)
    - [static_table](#static_table)
  - [Disable logging](#disable-logging)

-----

## Download Windows Application (GUI)

Choose the exe from the latest [release](https://github.com/UNSW-CEEM/NEMOSIS/releases)

-----

## Documentation

- Check out the [wiki](https://github.com/UNSW-CEEM/NEMOSIS/wiki)
- View worked examples:
  - [GUI](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Worked-Examples)
  - Python interface:
    - [Find a generator's Dispatch Unit ID (DUID) and download its SCADA data](https://github.com/UNSW-CEEM/NEMOSIS/blob/master/examples/generator_bidding_data.ipynb)
    - [Visualise a generators bidding behavour](https://github.com/UNSW-CEEM/NEMOSIS/blob/master/examples/generator_bidding_data.ipynb)
    - [replicate AEMO's analysis demonstrating the precision of battery dispatch](https://github.com/UNSW-CEEM/NEMOSIS/blob/master/examples/agc_target_vs_scada.ipynb)  
- What [data is available](https://github.com/UNSW-CEEM/NEMOSIS/wiki/AEMO-Tables) and data [column definitions](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Column-Summary)
- Watch a video
  - [Download generator dispatch data](https://youtu.be/HEAOk056Bss)
  - [Download dispatch data by fuel type](https://youtu.be/aKEI7URiJlI).
- Read our [paper](https://www.researchgate.net/publication/329798805_NEMOSIS_-_NEM_Open_Source_Information_Service_open-source_access_to_Australian_National_Electricity_Market_Data) introducting NEMOSIS

## Support NEMOSIS

Cite our [paper](https://www.researchgate.net/publication/329798805_NEMOSIS_-_NEM_Open_Source_Information_Service_open-source_access_to_Australian_National_Electricity_Market_Data) in your publications that use data from NEMOSIS.

## Get Updates, Ask Questions

Join the NEMOSIS [forum group](https://groups.google.com/forum/#!forum/nemosis-discuss).

-----

## Using the Python Interface (API)

### Installing NEMOSIS

`pip install nemosis`

### Data from dynamic tables

Dynamic tables contain a datetime column that allows NEMOSIS to filter their content by a start and end time. 

To learn more about each dynamic table visit the [wiki](https://github.com/UNSW-CEEM/NEMOSIS/wiki).

You can view the dynamic tables available by printing the NEMOSIS default settings.

```python

from src.nemosis import defaults

print(defaults.dynamic_tables)

# ['DISPATCHLOAD', 'DUDETAILSUMMARY', 'DUDETAIL', 'DISPATCHCONSTRAINT', 'GENCONDATA', 'DISPATCH_UNIT_SCADA', 'DISPATCHPRICE', . . .
```

#### Workflows

Your workflow may determine how you use NEMOSIS. Because the GUI relies on data being stored as strings (rather than numeric types such as integers or floats), we suggest the following:

- If you are using **NEMOSIS' API in your code, or using the same cache for the GUI and API**, use `dynamic_data_compiler`. This will allow your data to be handled by both the GUI and the API. Data read in via the API will be typed, i.e. datetime columns will be a datetime type, numeric columns will be integer/float, etc. See [this section](#dynamic-data-compiler).
- If you are using **NEMOSIS to cache data in feather or parquet format for use with another application**, use `cache_compiler`. This will ensure that cached feather/parquet files are appropriately typed to make further external processing easier. It will also cache faster as it doesn't prepare a DataFrame for further analysis. See [this section](#cache-compiler).

##### Dynamic data compiler

`dynamic_data_compiler` can be used to download and compile data from dynamic tables.

```python
from src.nemosis import dynamic_data_compiler

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:/Users/your_data_storage'

price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache)
```

Using the default settings of `dynamic_data_compiler` will download CSV data from AEMO's NEMWeb portal and save it to the `raw_data_cache` directory. It will also create a feather file version of each CSV (feather files have a faster read time). Subsequent `dynamic_data_compiler` calls will check if any data in `raw_data_cache` matches the query and loads it. This means that subsequent `dynamic_data_compiler` will be faster so long as the cached data is available.

A number of options are available to configure filtering (i.e. what data NEMOSIS returns as a pandas DataFrame) and caching.

###### Filter options

`dynamic_data_compiler` can be used to filter data before returning results.

To return only a subset of a particular table's columns, use the `select_columns` argument.

```python
from src.nemosis import dynamic_data_compiler

price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache,
                                   select_columns=['REGIONID', 'SETTLEMENTDATE', 'RRP'])
```

To see what columns a table has, you can inspect NEMOSIS' defaults.

```python

from src.nemosis import defaults

print(defaults.table_columns['DISPATCHPRICE'])
# ['SETTLEMENTDATE', 'REGIONID', 'INTERVENTION', 'RRP', 'RAISE6SECRRP', 'RAISE60SECRRP', 'RAISE5MINRRP', . . .
```

Columns can also be filtered by value. To do this, you need provide a column to be filtered (`filter_cols`) and a value or values to filter (`filter_values`) a corresponding column by. to filter by a column the column must be included as a filter column.

In the example below, the table will be filtered to only return rows where `REGIONID == 'SA1'`.

```python
from src.nemosis import dynamic_data_compiler

price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache, filter_cols=['REGIONID'],
                                   filter_values=(['SA1'],))
```

Several filters can be applied simultaneously. A common filter is to extract pricing data excluding any physical intervention dispatch runs (`INTERVENTION == 0` is the appropriate filter, see [here](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Column-Summary#intervention)). Below is an example of filtering to get data for Gladstone Unit 1 and Hornsdale Wind Farm 2 excluding any physical dispatch runs:

```python
from src.nemosis import dynamic_data_compiler

unit_dispatch_data = dynamic_data_compiler(start_time, end_time, 'DISPATCHLOAD', raw_data_cache,
                                           filter_cols=['DUID', 'INTERVENTION'],
                                           filter_values=(['GSTONE1', 'HDWF2'], [0]))
```

###### Caching options

By default the options fformat='feather' and keep_csv=True are used.

If the option fformat='csv' is used then no feather files will be created, and all caching will be done using CSVs.

```python
price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache, fformat='csv')
```

If you supply fformat='feather', the original AEMO CSVs will still be cached by default. To save disk space but still ensure your data will work with the API & GUI, use `keep_csv=False` in combination with `fformat='feather'` (which is the default option). This will delete the AEMO CSVs after the feather file is created.

```python
price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache, keep_csv=False)
```

If the option `fformat='parquet'` is provided then no feather files will be created, and a parquet file will be used instead.
While feather might have faster read/write, parquet has excellent compression characteristics and good compatability with packages for handling large on-memory/cluster datasets (e.g. Dask). This helps with local storage (especially for Causer Pays data) and file size for version control.

##### Cache compiler

This may be useful if you're using NEMOSIS to
build a data cache, but then process the cache using other packages or applications. It is particularly useful because `cache_compiler` will infer the data types of the columns before saving to parquet or feather, thereby eliminating the need to type convert data that is obtained using `dynamic_data_compiler`.

`cache_compiler` can be used to compile a cache of parquet or feather files. Parquet will likely be smaller, but feather can be read faster. `cache_compiler` will not run if it detects the appropriate files in the `raw_data_cache` directory. Otherwise, it will download CSVs, covert to the requested format and then delete the CSVs. It does not return any data, unlike `dynamic_data_compiler`.

The example below downloads parquet data into the cache.

```python
from src.nemosis import cache_compiler

cache_compiler(start_time, end_time, table, raw_data_cache, fformat='parquet')
```

##### Accessing additional table columns

By default NEMOSIS only includes a subset of an AEMO table's columns, the full set of columns are listed in the 
[MMS Data Model Reports](https://visualisations.aemo.com.au/aemo/di-help/Content/Data_Model/MMS_Data_Model.htm), 
or can be seen by inspecting the CSVs in the raw data cache. Users of the python interface can add additional 
columns as shown below. If you using a feather or parquet based cache the rebuild option should be set to
true so the additional columns are added to the cache files when they are rebuilt. This method of adding additional
columns should also work with the `cache_compiler` function.

```python
from src.nemosis import dynamic_data_compiler
from src.nemosis import defaults

defaults.table_columns['BIDPEROFFER_D'] += ['PASAAVAILABILITY']

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'BIDPEROFFER_D'
raw_data_cache = 'C:/Users/your_data_storage'

volume_bid_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache, rebuild=True)
```

### Data from static tables

Static tables do not include a time column and cannot be filtered by start and end time.

To learn more about each static table visit the [wiki](https://github.com/UNSW-CEEM/NEMOSIS/wiki).

You can view the static tables available by printing the tables in NEMOSIS' defaults:

```python

from src.nemosis import defaults

print(defaults.static_tables)
# ['ELEMENTS_FCAS_4_SECOND', 'VARIABLES_FCAS_4_SECOND', 'Generators and Scheduled Loads', 'FCAS Providers']
```

#### static_table

The `static_table` function can be used to access these tables

```python
from src.nemosis import static_table

fcas_variables = static_table('VARIABLES_FCAS_4_SECOND', raw_data_cache)
```
### Disable logging

NEMOSIS uses the python logging module to print messages to the console. If desired, this can be disabled after 
imports, as shown below. This will disable log messages unless they are at least warnings.

```python

import logging

from src.nemosis import dynamic_data_compiler

logging.getLogger("nemosis").setLevel(logging.WARNING)

```
