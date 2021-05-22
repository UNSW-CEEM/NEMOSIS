### NEMOSIS: An open-source tool to make it easier to download and analyise historical data published by AEMO 

### Download windows GUI
Choose the exe from the latest [release](https://github.com/UNSW-CEEM/NEMOSIS/releases)

### Documentation 
+ Check out the [wiki](https://github.com/UNSW-CEEM/NEMOSIS/wiki)
+ View [worked examples](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Worked-Examples)
+ What [data is available](https://github.com/UNSW-CEEM/NEMOSIS/wiki/AEMO-Tables) and data [column definitions](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Column-Summary)
+ Watch a video 
  * [Download generator dispatch data](https://youtu.be/HEAOk056Bss)
  * [Download dispatch data by fuel type](https://youtu.be/aKEI7URiJlI).
+ Read our [paper](https://www.researchgate.net/publication/329798805_NEMOSIS_-_NEM_Open_Source_Information_Service_open-source_access_to_Australian_National_Electricity_Market_Data) introducting NEMOSIS

### Support NEMOSIS
Cite our [paper](https://www.researchgate.net/publication/329798805_NEMOSIS_-_NEM_Open_Source_Information_Service_open-source_access_to_Australian_National_Electricity_Market_Data) in your publications that use data from NEMOSIS.

### Get Updates, Ask Questions
Join the NEMOSIS [forum group](https://groups.google.com/forum/#!forum/nemosis-discuss).

### Use the python interface
pip install nemosis

#### Data from dynamic tables
Dynamic tables contain a datetime column that allows NEMOSIS to filter their content by a start and end time. 

You can view the dynamic tables available by printing the NEMOSIS default settings.

```
from nemosis import defaults

print(defaults.dynamic_tables)
# ['DISPATCHLOAD', 'DUDETAILSUMMARY', 'DUDETAIL', 'DISPATCHCONSTRAINT', 'GENCONDATA', 'DISPATCH_UNIT_SCADA', 'DISPATCHPRICE', . . .
```

The dynamic_data_compiler can be used to download and compile data from dynamic tables.  

```
from nemosis import data_fetch_methods

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:/Users/your_data_storage'

price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache)
```

Using the dynamic_data_compiler's default settings will download CSV data from AEMO's nemweb portal and save it to 
the raw_data_cache directory, a feather file version of each CSV will also be create, feather files are faster to read 
from disk, this means subsequent dynamic_data_compiler calls that require the cached data will be faster.

A number of options are available to configure how NEMOSIS handles caching. 

##### Cache formatting options
By default the options fformat='feather', keep_csv=True and data_merge=True are used.

If the option fformat='csv' is used then no feather files will be created, and all caching will be done using CSVs.

```
price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache, fformat='csv')
```

If the option fformat='parquet' is provided then no feather files will be created, and a parquet file will be used instead. 
While feather might have faster read/write, parquet has excellent compression characteristics and good compatability 
with packages for handling large on-memory/cluster datasets (e.g. Dask). This helps with local storage 
(especially for Causer Pays data) and file size for version control. This may be useful if you're using NEMOSIS to
build the data cache but then processing the cache using other packages.

```
price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache, fformat='parquet')
```

By default the original AEMO CSVs will still be cached. To get the most benefit for saving disk space you can also 
use keep_csv=False in combination with fformat='parquet'. This will delete the AEMO CSVs after the parquet file 
is created.

```
price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache, fformat='parquet',
                                                      keep_csv=False)
```

If you're just using the dynamic_data_compiler to build the cache, the process can be sped up by setting merge_data=False, this
will download and build the cache but not merge the results from each cache file, in this case a None value will be 
returned.

```
data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache, merge_data=False)
```

##### Filter options
The dynamic_data_compiler can be used to filter data before returning results.

To return only a subsets of the tables columns uses the select_columns argument.

```
data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache,
                                         select_columns=['REGIONID', 'SETTLEMENTDATE', 'RRP'])
```

To see what columns a table has you can inspect the NEMOSIS defaults.

```
from nemosis import defaults

print(defaults.table_columns['DISPATCHPRICE'])
# ['SETTLEMENTDATE', 'REGIONID', 'INTERVENTION', 'RRP', 'RAISE6SECRRP', 'RAISE60SECRRP', 'RAISE5MINRRP', . . .
```

Columns can also be filtered by value. Note to filter by a column the column must be included as a filter column.
In the example below the table will be filter to just rows where REGIONID == 'SA1'.

```
data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache,
                                         filter_cols=['REGIONID'], filter_values=(['SA1'],))
```

#### Data from static tables
Static tables do not include a time column and cannot be filtered by start and end time.

You can view the dynamic tables available by printing the NEMOSIS default settings. To learn more about each static
table visit the wiki.

```
from nemosis import defaults

print(defaults.static_tables)
# ['ELEMENTS_FCAS_4_SECOND', 'VARIABLES_FCAS_4_SECOND', 'Generators and Scheduled Loads', 'FCAS Providers']
```

##### static_table_xl
The static_table_xl function can be used to access the tables 'Generators and Scheduled Loads' and 'FCAS Providers'.

```
gens = data_fetch_methods.static_table_xl('Generators and Scheduled Loads', raw_data_cache)
```

##### static_table
The static_table function can be used to access the table 'VARIABLES_FCAS_4_SECOND'.

```
fcas_variables = data_fetch_methods.static_table('VARIABLES_FCAS_4_SECOND', raw_data_cache)
```

##### static_table_FCAS_elements_file
The static_table_FCAS_elements_file function can be used to access the table 'ELEMENTS_FCAS_4_SECOND'.

```
fcas_variables = data_fetch_methods.static_table_FCAS_elements_file('ELEMENTS_FCAS_4_SECOND', raw_data_cache)
```
