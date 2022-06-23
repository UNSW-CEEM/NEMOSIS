from nemosis import dynamic_data_compiler, defaults


# bid_data = dynamic_data_compiler(start_time='2021/06/02 02:00:00', end_time='2021/06/02 03:00:00',
#                                  table_name='BIDPEROFFER_D', raw_data_location='D:/temp_cache',
#                                  fformat='parquet')
#
# print(bid_data)

# bid_data = dynamic_data_compiler(start_time='2018/02/01 00:00:00', end_time='2018/02/01 05:15:00',
#                                  table_name='DISPATCHLOAD', raw_data_location='D:/temp_cache',
#                                  fformat='feather', keep_csv=False)

price_data = dynamic_data_compiler('2019/01/01 02:00:00', '2019/01/01 03:00:00', 'DISPATCHPRICE',
                                   'D:/temp_cache')

print(price_data)