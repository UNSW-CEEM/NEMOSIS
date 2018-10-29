# OSDAN

# Use OSDAN without python
Download the latest executable file [here](https://github.com/UNSW-CEEM/osdan/releases)

# Use the source code

```
from osdan import data_fetch_methods

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:00:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:\Users\your_data_storage'

data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache)

