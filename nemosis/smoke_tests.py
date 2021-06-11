from nemosis import dynamic_data_compiler, static_table, cache_compiler
import pandas as pd

start_time = '2017/01/01 00:00:00'
end_time = '2017/01/01 00:05:00'
table = 'DISPATCHPRICE'
raw_data_cache = 'C:/Users/nick/Desktop/cache'

# price_data = dynamic_data_compiler(start_time, end_time, table, raw_data_cache, fformat='parquet', keep_csv=False,
#                                    parse_data_types=True)
#
# print(price_data)
#
# gens = static_table('Generators and Scheduled Loads', raw_data_cache)
#
# print(gens)


# cache_compiler(start_time, end_time, table, raw_data_cache, fformat='feather',
#                select_columns=['REGIONID', 'RRP'], rebuild=True, keep_csv=True)
#
# t = pd.read_feather('smoke_cache\PUBLIC_DVD_DISPATCHPRICE_201612010000.feather')
#
# print(t['RRP'].iloc[0])
# print(t)

duids = static_table('Generators and Scheduled Loads', raw_data_cache)

duids.to_csv('C:/Users/nick/Desktop/test.csv')