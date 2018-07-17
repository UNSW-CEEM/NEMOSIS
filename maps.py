import filters
import downloader
import query_wrapers
import write_file_names
import date_generators

setup_map = {'DISPATCHLOAD': None,
             'DISPATCHPRICE': None,
             'DISPATCH_UNIT_SCADA': None,
             'DISPATCHCONSTRAINT': None,
             'DUDETAILSUMMARY': None,
             'GENCONDATA': None,
             'STATION': None,
             'STADUALLOC': None,
             'GENUNITS': None,
             'PARTICIPANT': None,
             'SPDREGIONCONSTRAINT': None,
             'SPDCONNECTIONPOINTCONSTRAINT': None,
             'SPDINTERCONNECTORCONSTRAINT': None,
             'FCAS_4_SECOND': None,
             'ELEMENTS_FCAS_4_SECOND': None,
             'VARIABLES_FCAS_4_SECOND': None,
             'MASTER_REGISTRATION_LIST': None,
             'BIDDAYOFFER_D': query_wrapers.dispatch_date_setup,
             'BIDPEROFFER_D': None,
             'FCAS_4s_SCADA_MAP': None}


search_type_map = {'DISPATCHLOAD': 'start_to_end',
             'DISPATCHPRICE': 'start_to_end',
             'DISPATCH_UNIT_SCADA': 'start_to_end',
             'DISPATCHCONSTRAINT': 'start_to_end',
             'DUDETAILSUMMARY': 'last',
             'GENCONDATA': 'all',
             'STATION': 'all',
             'STADUALLOC': 'last',
             'GENUNITS': 'all',
             'PARTICIPANT': 'all',
             'SPDREGIONCONSTRAINT': 'all',
             'SPDCONNECTIONPOINTCONSTRAINT': 'all',
             'SPDINTERCONNECTORCONSTRAINT': 'all',
             'FCAS_4_SECOND': 'start_to_end',
             'ELEMENTS_FCAS_4_SECOND': None,
             'VARIABLES_FCAS_4_SECOND': None,
             'MASTER_REGISTRATION_LIST': None,
             'BIDDAYOFFER_D': 'start_to_end',
             'BIDPEROFFER_D': 'start_to_end',
             'FCAS_4s_SCADA_MAP': None}


filter_map = {'DISPATCHLOAD': filters.filter_on_settlementdate,
             'DISPATCHPRICE': filters.filter_on_settlementdate,
             'DISPATCH_UNIT_SCADA': filters.filter_on_settlementdate,
             'DISPATCHCONSTRAINT': filters.filter_on_settlementdate,
             'DUDETAILSUMMARY': filters.filter_on_start_and_end_date,
             'GENCONDATA': filters.filter_on_effective_date,
             'STATION': None,
             'STADUALLOC': filters.filter_on_effective_date,
             'GENUNITS': None,
             'PARTICIPANT': None,
             'SPDREGIONCONSTRAINT': filters.filter_on_effective_date,
             'SPDCONNECTIONPOINTCONSTRAINT': filters.filter_on_effective_date,
             'SPDINTERCONNECTORCONSTRAINT': filters.filter_on_effective_date,
             'FCAS_4_SECOND': filters.filter_on_timestamp,
             'ELEMENTS_FCAS_4_SECOND': None,
             'VARIABLES_FCAS_4_SECOND': None,
             'MASTER_REGISTRATION_LIST': None,
             'BIDDAYOFFER_D':filters.filter_on_settlementdate,
             'BIDPEROFFER_D': filters.filter_on_settlementdate,
             'FCAS_4s_SCADA_MAP': None}


finalise_map = {'DISPATCHLOAD': None,
                'DISPATCHPRICE': None,
                'DISPATCH_UNIT_SCADA': None,
                'DISPATCHCONSTRAINT': None,
                'DUDETAILSUMMARY': query_wrapers.start_and_end_finalise,
                'GENCONDATA': None,
                'STATION': None,
                'STADUALLOC': query_wrapers.effective_date_finalise,
                'GENUNITS': None,
                'PARTICIPANT': None,
                'SPDREGIONCONSTRAINT': query_wrapers.effective_date_finalise,
                'SPDCONNECTIONPOINTCONSTRAINT': query_wrapers.effective_date_finalise,
                'SPDINTERCONNECTORCONSTRAINT': query_wrapers.effective_date_finalise,
                'FCAS_4_SECOND': query_wrapers.fcas4s_finalise,
                'ELEMENTS_FCAS_4_SECOND': None,
                'VARIABLES_FCAS_4_SECOND': None,
                'MASTER_REGISTRATION_LIST': None,
                'BIDDAYOFFER_D': None,
                'BIDPEROFFER_D': None,
                'FCAS_4s_SCADA_MAP': None}



date_gen_map = {'MMS': date_generators.year_and_month_gen,
                'FCAS': date_generators.year_month_day_index_gen}

write_filename_map = {'MMS': write_file_names.write_file_names,
                      'FCAS': write_file_names.write_file_names_fcas}

downloader_map = {'MMS': downloader.run,
                  'FCAS': downloader.run_fcas4s()}