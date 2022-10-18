import logging

from nemosis import dynamic_data_compiler

logging.getLogger("nemosis").setLevel(logging.WARNING)

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'D:/nemosis_cache'

price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache)