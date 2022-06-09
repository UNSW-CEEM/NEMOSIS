from nemosis import dynamic_data_compiler, defaults


bid_data = dynamic_data_compiler(start_time='2021/02/01 00:00:00', end_time='2021/05/01 00:00:00',
                                 table_name='BIDPEROFFER_D', raw_data_location='D:/temp_cache',
                                 fformat='parquet')

x=1