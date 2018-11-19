# NEMOSIS
Check out the wiki! This is the best source of info on [how to](https://github.com/UNSW-CEEM/NEMOSIS/wiki) use the tool, [worked examples](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Worked-Examples), what [data is available](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Table-Summary) and data [column definitions](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Column-Summary).  

# Use NEMOSIS without python
Download the latest executable file [here](https://github.com/UNSW-CEEM/NEMOSIS/releases)

# Use the source code
pip install nemosis

```
from nemosis import data_fetch_methods

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:00:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:\Users\your_data_storage'

price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache)

