import filters
import pandas as pd
from datetime import timedelta, datetime
import data_fetch_methods
import math
import defaults
import numpy as np

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
    best_matches_scada = error_comp[error_comp['SCADAVALUE'].abs() > 0]  # Don't include units 0 values for scada
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
    scada_data = np.where(capacity_and_scada_grouped['SCADAVALUE'].isnull(), 0.0,
                          capacity_and_scada_grouped['SCADAVALUE'])
    cf = scada_data / capacity_and_scada_grouped['MAXCAPACITY']
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
    return volume_weighted_average_price(capacity_and_scada_grouped['TRADING_TOTALCLEARED'],
                                         capacity_and_scada_grouped['TRADING_RRP'])


def volume_weighted_average_spot_price(capacity_and_scada_grouped):
    return volume_weighted_average_price(capacity_and_scada_grouped['SCADAVALUE'],
                                         capacity_and_scada_grouped['DISPATCH_RRP'])


def performance_at_nodal_peak(capacity_and_scada_grouped):
    index_max = capacity_and_scada_grouped['TOTALDEMAND'].idxmax()
    output_at_peak = capacity_and_scada_grouped['SCADAVALUE'][index_max]
    if np.isnan(output_at_peak):
        output_at_peak = 0
    performance = output_at_peak / capacity_and_scada_grouped['MAXCAPACITY'][index_max]
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
    v = volume(capacity_and_scada_grouped)
    tvwap = volume_weighted_average_trading_price(capacity_and_scada_grouped)
    dvwap = volume_weighted_average_spot_price(capacity_and_scada_grouped)
    peak = performance_at_nodal_peak(capacity_and_scada_grouped)
    peak_percentile = capacity_factor_over_90th_percentile_of_nodal_demand(capacity_and_scada_grouped)
    month = list(capacity_and_scada_grouped['MONTH'])[0]
    duid = list(capacity_and_scada_grouped['DUID'])[0]
    cf_df = pd.DataFrame({'Month': [month], 'DUID': [duid], 'CapacityFactor': [cf], 'Volume': [v],
                          'TRADING_VWAP': [tvwap], 'DISPATCH_VWAP': [dvwap], 'NodalPeakCapacityFactor': peak,
                          'Nodal90thPercentileCapacityFactor': [peak_percentile]})
    return cf_df


def stats_by_month_and_plant(capacity_and_scada):
    capacity_and_scada['MONTH'] = capacity_and_scada['SETTLEMENTDATE'].dt.year.astype(str) + '-' + \
                                  capacity_and_scada['SETTLEMENTDATE'].dt.month.astype(str).str.zfill(2)
    capacity_factors = capacity_and_scada.groupby(['MONTH', 'DUID'], as_index=False).apply(stats_for_group)
    return capacity_factors


def merge_tables_for_plant_stats(timeseries_df, gen_max_cap, gen_region, scada, trading_load, dispatch_price,
                                 trading_price, region_summary):
    gen_max_cap = gen_max_cap.sort_values('EFFECTIVEDATE')
    gen_max_cap = gen_max_cap[gen_max_cap['DUID'].isin(scada['DUID'])]
    merged_data_temp = []
    for gen in gen_max_cap.groupby(['DUID', 'EFFECTIVEDATE']):
        merged_data_temp.append(pd.merge_asof(timeseries_df, gen[1], left_on='SETTLEMENTDATE', right_on='EFFECTIVEDATE'))
    merged_data = pd.concat(merged_data_temp)
    merged_data = merged_data.sort_values('SETTLEMENTDATE')
    gen_region = gen_region.sort_values('START_DATE')
    merged_data = pd.merge_asof(merged_data, gen_region, left_on='SETTLEMENTDATE', right_on='START_DATE', by='DUID')
    merged_data = pd.merge(merged_data, trading_load, 'left', on=['SETTLEMENTDATE', 'DUID'])
    merged_data = pd.merge(merged_data, dispatch_price, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = pd.merge(merged_data, trading_price, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = pd.merge(merged_data, region_summary, 'left', on=['SETTLEMENTDATE', 'REGIONID'])
    merged_data = pd.merge(merged_data, scada, 'left', on=['SETTLEMENTDATE', 'DUID'])
    merged_data = merged_data.loc[:, ('DUID', 'EFFECTIVEDATE', 'REGIONID', 'SETTLEMENTDATE', 'MAXCAPACITY',
                                      'SCADAVALUE', 'TOTALCLEARED', 'RRP_x', 'RRP_y', 'TOTALDEMAND')]
    merged_data.columns = ['DUID', 'DUDETAIL_EFFECTIVEDATE', 'REGIONID', 'SETTLEMENTDATE', 'MAXCAPACITY', 'SCADAVALUE',
                           'TRADING_TOTALCLEARED', 'DISPATCH_RRP', 'TRADING_RRP', 'TOTALDEMAND']
    return merged_data


def select_intervention_if_present(data, primary_key):
    data = data.sort_values(['INTERVENTION'])
    data = data.groupby([col for col in primary_key if col != 'INTERVENTION'], as_index=False).last()
    return data


def select_highest_version_number(data, primary_key):
    data['VERSIONNO'] = pd.to_numeric(data['VERSIONNO'])
    data = data.sort_values(['VERSIONNO'])
    data['VERSIONNO'] = data['VERSIONNO'].astype(int).astype(str)
    data = data.groupby([col for col in primary_key if col != 'INTERVENTION'], as_index=False).last()
    return data


def plant_stats(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                filter_values=None):
    ix = pd.DatetimeIndex(start=datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S'),
                          end=datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S') - timedelta(minutes=5),
                          freq='5T')
    timeseries_df = pd.DataFrame(index=ix)
    timeseries_df.reset_index(inplace=True)
    timeseries_df.columns = ['SETTLEMENTDATE']
    gen_max_cap = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAIL', raw_data_location,
                                                           select_columns=['EFFECTIVEDATE', 'DUID', 'VERSIONNO',
                                                                           'MAXCAPACITY'], filter_cols=filter_cols,
                                                           filter_values=filter_values)
    gen_max_cap = select_highest_version_number(gen_max_cap, defaults.table_primary_keys['DUDETAIL'])
    gen_region = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAILSUMMARY', raw_data_location,
                                                          select_columns=['START_DATE', 'END_DATE', 'DUID', 'REGIONID'])
    scada = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCH_UNIT_SCADA', raw_data_location,
                                                     select_columns=['SETTLEMENTDATE', 'DUID', 'SCADAVALUE'])
    trading_load = scada.copy()
    trading_load['timestamp'] = trading_load['SETTLEMENTDATE']
    trading_load = trading_load.set_index('timestamp')
    trading_load['SCADAVALUE'] = pd.to_numeric(trading_load['SCADAVALUE'])
    trading_load = trading_load.groupby('DUID').resample('30T', label='right', closed='right').aggregate(
        {'SCADAVALUE': 'mean', 'SETTLEMENTDATE': 'last'})
    trading_load.reset_index(inplace=True)
    trading_load = trading_load.drop('timestamp', axis=1)
    trading_load.columns = ['DUID', 'TOTALCLEARED', 'SETTLEMENTDATE']
    #  trading_load = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'TRADINGLOAD', raw_data_location,
    #                                                       select_columns=['SETTLEMENTDATE', 'DUID', 'TOTALCLEARED'])
    dispatch_price = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHPRICE', raw_data_location,
                                                              select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP',
                                                                              'INTERVENTION'])
    dispatch_price = select_intervention_if_present(dispatch_price, defaults.table_primary_keys['DISPATCHPRICE'])
    trading_price = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'TRADINGPRICE', raw_data_location,
                                                             select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP'])
    region_summary = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHREGIONSUM',
                                                              raw_data_location,
                                                              select_columns=['SETTLEMENTDATE', 'REGIONID',
                                                                              'TOTALDEMAND', 'INTERVENTION',
                                                                              'DISPATCHINTERVAL'])

    region_summary = select_intervention_if_present(region_summary, defaults.table_primary_keys['DISPATCHREGIONSUM'])

    combined_data = merge_tables_for_plant_stats(timeseries_df, gen_max_cap, gen_region, scada, trading_load,
                                                 dispatch_price, trading_price, region_summary)
    combined_data['SCADAVALUE'] = pd.to_numeric(combined_data['SCADAVALUE'])
    combined_data['MAXCAPACITY'] = pd.to_numeric(combined_data['MAXCAPACITY'])
    combined_data['TRADING_TOTALCLEARED'] = pd.to_numeric(combined_data['TRADING_TOTALCLEARED'])
    combined_data['TRADING_RRP'] = pd.to_numeric(combined_data['TRADING_RRP'])
    combined_data['DISPATCH_RRP'] = pd.to_numeric(combined_data['DISPATCH_RRP'])
    combined_data['TOTALDEMAND'] = pd.to_numeric(combined_data['TOTALDEMAND'])
    stats = stats_by_month_and_plant(combined_data)
    return stats