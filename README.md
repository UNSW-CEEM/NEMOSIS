
### Download windows GUI
Choose the exe from the latest [release](https://github.com/UNSW-CEEM/NEMOSIS/releases)

### Documentation 
+ Check out the [wiki](https://github.com/UNSW-CEEM/NEMOSIS/wiki)
+ View [worked examples](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Worked-Examples)
+ What [data is available](https://github.com/UNSW-CEEM/NEMOSIS/wiki/AEMO-Tables) and data [column definitions](https://github.com/UNSW-CEEM/NEMOSIS/wiki/Column-Summary)
+ Watch a [tutorial video](https://youtu.be/HEAOk056Bss).

### Get Updates, Ask Questions
Join the NEMOSIS [forum group](https://groups.google.com/forum/#!forum/nemosis-discuss).

### Use the python interface
pip install nemosis

```
from nemosis import data_fetch_methods

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:/Users/your_data_storage'

price_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table, raw_data_cache)

