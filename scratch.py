import logging

from nemosis import data_fetch_methods

#logging.getLogger("nemosis").setLevel(logging.WARNING)

start_time = "2013/12/23 19:05:00"
end_time = "2013/12/23 19:10:00"
table = 'Generators and Scheduled Loads'
raw_data_cache = 'D:/test'

bid_data = data_fetch_methods.static_table(
                table,
                raw_data_cache,
                update_static_file=True
            )

print(bid_data)