import filters
import pandas as pd
from datetime import timedelta, datetime
import data_fetch_methods
import math


def fcas4s_scada_match(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                       filter_values=None):

    # Pull in the 4 second fcas data.
    table_name_fcas4s = 'FCAS_4_SECOND'
    fcas4s = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table_name_fcas4s, raw_data_location)
    # Pull in the 4 second fcas variable types.
    table_name_variable_types = 'VARIABLES_FCAS_4_SECOND'
    fcas4s_variable_types = data_fetch_methods.static_table(start_time, end_time, table_name_variable_types,
                                                            raw_data_location)

    # Select the variable types that measure MW on an interconnector and Gen_MW from a dispatch unit.
    fcas4s_variable_types = fcas4s_variable_types[fcas4s_variable_types['VARIABLETYPE'].isin(['MW', 'Gen_MW'])]
    fcas4s = fcas4s[fcas4s['VARIABLENUMBER'].isin(fcas4s_variable_types['VARIABLENUMBER'])]

    # Select just the fcas 4 second data variable columns that we need.
    fcas4s = fcas4s.loc[:, ('TIMESTAMP', 'ELEMENTNUMBER', 'VALUE')]

    # Convert the fcas MW measured values to numeric type.
    fcas4s['VALUE'] = pd.to_numeric(fcas4s['VALUE'])

    # Rename the 4 second measurements to the timestamp of the start of the 5 min interval i.e round down to nearest
    # 5 min interval.
    fcas4s = fcas4s[(fcas4s['TIMESTAMP'].dt.minute.isin(list(range(0, 60, 5)))) &
                    (fcas4s['TIMESTAMP'].dt.second < 20)]
    fcas4s['TIMESTAMP'] = fcas4s['TIMESTAMP'].apply(lambda dt: datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))

    # Pull in the dispatch unit scada data.
    table_name_scada = 'DISPATCH_UNIT_SCADA'
    scada = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table_name_scada, raw_data_location)
    scada['SETTLEMENTDATE'] = scada['SETTLEMENTDATE'] - timedelta(minutes=5)
    scada = scada.loc[:, ('SETTLEMENTDATE', 'DUID', 'SCADAVALUE')]
    scada.columns = ['SETTLEMENTDATE', 'MARKETNAME', 'SCADAVALUE']
    scada['SCADAVALUE'] = pd.to_numeric(scada['SCADAVALUE'])

    # Pull in the interconnector scada data and use the intervention records where the exist.
    table_name_inter_flow = 'DISPATCHINTERCONNECTORRES'
    inter_flows = data_fetch_methods.dynamic_data_compiler(start_time, end_time, table_name_inter_flow,
                                                           raw_data_location)
    inter_flows['METEREDMWFLOW'] = pd.to_numeric(inter_flows['METEREDMWFLOW'])
    inter_flows = inter_flows.sort_values('INTERVENTION')
    inter_flows = inter_flows.groupby(['SETTLEMENTDATE', 'INTERCONNECTORID'], as_index=False).last()
    inter_flows = inter_flows.loc[:, ('SETTLEMENTDATE', 'INTERCONNECTORID', 'METEREDMWFLOW')]
    inter_flows['SETTLEMENTDATE'] = inter_flows['SETTLEMENTDATE'] - timedelta(minutes=5)
    inter_flows.columns = ['SETTLEMENTDATE', 'MARKETNAME', 'SCADAVALUE']

    # Combine scada data from interconnectors and dispatch units.
    scada_elements = pd.concat([scada, inter_flows], sort=False)

    # Merge the fcas and scada data based on time stamp, these leads every scada element to be joined to every fcas
    # element that then allows them to be comapred.
    profile_comp = pd.merge(fcas4s, scada_elements, 'inner', left_on='TIMESTAMP', right_on='SETTLEMENTDATE')

    # Calculate the error between each measurement.
    profile_comp['ERROR'] = profile_comp['VALUE'] - profile_comp['SCADAVALUE']
    profile_comp['ERROR'] = profile_comp['ERROR'].abs()

    # Choose the fcas values that best matches the scada value during the 5 min interval.
    profile_comp = profile_comp.sort_values('ERROR')
    error_comp = profile_comp.groupby(['MARKETNAME', 'ELEMENTNUMBER', 'TIMESTAMP'], as_index=False).first()

    # Aggregate the error to comapre each scada and fcas element potential match.
    error_comp = error_comp.groupby(['MARKETNAME', 'ELEMENTNUMBER'], as_index=False).sum()

    # Sort the comparisons based on aggregate error.
    error_comp = error_comp.sort_values('ERROR')

    # Drop duplicates of element numbers and scada element names, keeping the record for each with the least error.
    best_matches_scada = error_comp[error_comp['SCADAVALUE'].abs() > 0] # Don't include units 0 values for scada
    best_matches_scada = best_matches_scada.drop_duplicates('ELEMENTNUMBER', keep='first')
    best_matches_scada = best_matches_scada.drop_duplicates('MARKETNAME', keep='first')

    # Remove fcas elements where a match only occurred because both fcas and scada showed no dispatch.
    best_matches_scada['ELEMENTNUMBER'] = pd.to_numeric(best_matches_scada['ELEMENTNUMBER'])
    best_matches_scada = best_matches_scada.sort_values('ELEMENTNUMBER')
    best_matches_scada['ELEMENTNUMBER'] = best_matches_scada['ELEMENTNUMBER'].astype(str)

    # Give error as a percentage.
    best_matches_scada['ERROR'] = best_matches_scada['ERROR'] / best_matches_scada['SCADAVALUE']

    # drop matches with error greater than 100 %
    best_matches_scada = best_matches_scada[(best_matches_scada['ERROR'] < 1) & (best_matches_scada['ERROR'] > -1)]

    best_matches_scada = best_matches_scada.loc[:, ('ELEMENTNUMBER', 'MARKETNAME', 'ERROR')]

    if select_columns is not None:
        best_matches_scada = best_matches_scada.loc[:, select_columns]

    if filter_cols is not None:
        best_matches_scada = filters.filter_on_column_value(best_matches_scada, filter_cols, filter_values)

    return best_matches_scada


def capacity_factor(capacity_and_scada_grouped):
    cf = capacity_and_scada_grouped['SCADAVALUE']/capacity_and_scada_grouped['MAXCAPACITY']
    cf = cf.mean()
    return cf


def volume(capacity_and_scada_grouped):
    # Assumes 5 min Scada data
    return capacity_and_scada_grouped['SCADAVALUE'].sum()/12


def volume_weighted_average_price(output, prices):
    v_by_p = output * prices
    total_cash = v_by_p.sum()
    total_volume = output.sum()
    vwap = total_cash / total_volume
    return vwap


def volume_weighted_average_trading_price(capacity_and_scada_grouped):
    return volume_weighted_average_price(capacity_and_scada_grouped['TRADINGTOTALCLEARED'],
                                         capacity_and_scada_grouped['TRADINGRRP'])


def volume_weighted_average_spot_price(capacity_and_scada_grouped):
    return volume_weighted_average_price(capacity_and_scada_grouped['SCADAVALUE'],
                                         capacity_and_scada_grouped['DISPATCHRRP'])


def performance_at_nodal_peak(capacity_and_scada_grouped):
    index_max = capacity_and_scada_grouped['TOTALDEMAND'].idxmax()
    performance = capacity_and_scada_grouped['SCADAVALUE'].iloc[index_max] / \
        capacity_and_scada_grouped['MAXCAPACITY'].iloc[index_max]
    return performance


def capacity_factor_over_90th_percentile_of_nodal_demand(capacity_and_scada_grouped):
    data_entries = len(capacity_and_scada_grouped['TOTALDEMAND'])
    enteries_in_90th_percentile = math.ceil(data_entries/10)
    capacity_and_scada_grouped = capacity_and_scada_grouped.sort_values('TOTALDEMAND', ascending=False)
    capacity_and_scada_grouped = capacity_and_scada_grouped.reset_index(drop=True)
    capacity_and_scada_grouped = capacity_and_scada_grouped.iloc[:enteries_in_90th_percentile, :]
    cf = capacity_factor(capacity_and_scada_grouped)
    return cf


def stats_for_group(capacity_and_scada_grouped):
    cf = capacity_factor(capacity_and_scada_grouped)
    month = list(capacity_and_scada_grouped['MONTH'])[0]
    duid = list(capacity_and_scada_grouped['DUID'])[0]
    cf_df = pd.DataFrame({'MONTH': [month], 'DUID': [duid], 'CapacityFactor': [cf]})
    return cf_df


def stats_by_month_and_plant(capacity_and_scada):
    capacity_and_scada['MONTH'] = capacity_and_scada['SETTLEMENTDATE'].dt.year.astype(str) + '-' + \
                                  capacity_and_scada['SETTLEMENTDATE'].dt.month.astype(str).str.zfill(2)
    capacity_factors = capacity_and_scada.groupby(['MONTH', 'DUID'], as_index=False).apply(stats_for_group)
    return capacity_factors


def merge_tables_for_plant_stats(gen_info, scada, trading_load, dispatch_price, trading_price, region_summary):
    scada = scada.sort_values('SETTLEMENTDATE')
    gen_info = gen_info.sort_values('EFFECTIVEDATE')
    merged_data = pd.merge_asof(scada, gen_info, left_on='SETTLEMENTDATE', right_on='EFFECTIVEDATE', by='DUID')
    merged_data = pd.merge(merged_data, trading_load, 'left', on=['SETTLEMENTDATE', 'DUID'])
    merged_data = pd.merge(merged_data, dispatch_price, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = pd.merge(merged_data, trading_price, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = pd.merge(merged_data, region_summary, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = merged_data.loc[:, ('DUID', 'EFFECTIVEDATE', 'REGIONID', 'SETTLEMENTDATE', 'MAXCAPACITY',
                                      'SCADAVALUE', 'TOTALCLEARED', 'RRP_x', 'RRP_y', 'TOTALDEMAND')]
    merged_data.columns = ['DUID', 'DUDETAIL_EFFECTIVEDATE', 'REGIONID', 'SETTLEMENTDATE', 'MAXCAPACITY', 'SCADAVALUE',
                           'TRADING_TOTALCLEARED', 'DISPATCH_RRP', 'TRADING_RRP', 'TOTALDEMAND']
    return merged_data


def plant_stats(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                filter_values=None):
    gen_info = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAIL', raw_data_location,
                                                        select_columns=['EFFECTIVEDATE', 'DUID', 'MAXCAPACITY'])
    scada = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCH_UNIT_SCADA', raw_data_location,
                                                     select_columns=['SETTLEMENTDATE', 'DUID', 'SCADAVALUE'])
    trading_load = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'TRADINGLOAD', raw_data_location,
                                                            select_columns=['SETTLEMENTDATE', 'DUID', 'TOTALCLEARED'])
    dispatch_price = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHPRICE', raw_data_location,
                                                              select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP'])
    trading_price = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAIL', raw_data_location,
                                                             select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP'])
    region_summary = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAIL', raw_data_location,
                                                              select_columns=['SETTLEMENTDATE', 'REGIONID',
                                                                              'TOTALDEMAND'])
    combined_data = merge_tables_for_plant_stats(gen_info, scada, trading_load, dispatch_price, trading_price,
                                                 region_summary)
    stats = stats_by_month_and_plant(combined_data)
    return stats