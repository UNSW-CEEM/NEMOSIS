import standard_queries
import pandas as pd
from datetime import timedelta, datetime
import query_wrapers


def fcas4s_scada_match(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                       filter_values=None):

    # Pull in the 4 second fcas data.
    table_name_fcas4s = 'FCAS_4_SECOND'
    fcas4s = standard_queries.compile_generic_fcas(start_time, end_time, table_name_fcas4s, raw_data_location,
                                                   standard_queries.filter_on_timestamp, search='start_to_end')
    # Pull in the 4 second fcas variable types.
    table_name_variable_types = 'VARIABLES_FCAS_4_SECOND'
    fcas4s_variable_types = query_wrapers.static_table(start_time, end_time, table_name_variable_types, raw_data_location)

    # Select the variable types that measure MW on an interconnector and Gen_MW from a dispatch unit.
    fcas4s_variable_types = fcas4s_variable_types[fcas4s_variable_types['VARIABLETYPE'].isin(['MW', 'Gen_MW'])]
    fcas4s = fcas4s[fcas4s['VARIABLENUMBER'].isin(fcas4s_variable_types['VARIABLENUMBER'])]

    # Select just the fcas 4 second data variable columns that we need.
    fcas4s = fcas4s.loc[:, ('TIMESTAMP', 'ELEMENTNUMBER', 'VALUE')]

    # Convert the fcas MW measured values to numeric type.
    fcas4s['VALUE'] = pd.to_numeric(fcas4s['VALUE'])

    # Rename the 4 second measurements to the timestamp of the start of the 5 min interval i.e round down to nearest
    # 5 min interval.
    fcas4s['TIMESTAMP'] = fcas4s['TIMESTAMP'].apply(lambda dt: datetime(dt.year, dt.month, dt.day, dt.hour, 5 * (dt.minute // 5)))

    # Pull in the dispatch unit scada data.
    table_name_scada = 'DISPATCH_UNIT_SCADA'
    scada = query_wrapers.dispatch(start_time, end_time, table_name_scada, raw_data_location)
    scada['SETTLEMENTDATE'] = scada['SETTLEMENTDATE'] - timedelta(minutes=5)
    scada = scada.loc[:, ('SETTLEMENTDATE', 'DUID', 'SCADAVALUE')]
    scada.columns = ['SETTLEMENTDATE', 'SCADA_ELEMENT', 'SCADAVALUE']
    scada['SCADAVALUE'] = pd.to_numeric(scada['SCADAVALUE'])

    # Pull in the interconnector scada data and use the intervention records where the exist.
    table_name_inter_flow = 'DISPATCHINTERCONNECTORRES'
    inter_flows = query_wrapers.dispatch(start_time, end_time, table_name_inter_flow, raw_data_location)
    inter_flows['METEREDMWFLOW'] = pd.to_numeric(inter_flows['METEREDMWFLOW'])
    inter_flows = inter_flows.sort_values('INTERVENTION')
    inter_flows = inter_flows.groupby(['SETTLEMENTDATE', 'INTERCONNECTORID'], as_index=False).last()
    inter_flows = inter_flows.loc[:, ('SETTLEMENTDATE', 'INTERCONNECTORID', 'METEREDMWFLOW')]
    inter_flows['SETTLEMENTDATE'] = inter_flows['SETTLEMENTDATE'] - timedelta(minutes=5)
    inter_flows.columns = ['SETTLEMENTDATE', 'SCADA_ELEMENT', 'SCADAVALUE']

    # Combine scada data from interconnectors and dispatch units.
    scada_elements = pd.concat([scada, inter_flows])

    # Merge the fcas and scada data based on time stamp, these leads every scada element to be joined to every fcas
    # element that then allows them to be comapred.
    profile_comp = pd.merge(fcas4s, scada_elements, 'inner', left_on='TIMESTAMP', right_on='SETTLEMENTDATE')

    # Calculate the error between each measurement.
    profile_comp['ERROR'] = profile_comp['VALUE'] - profile_comp['SCADAVALUE']
    profile_comp['ERROR'] = profile_comp['ERROR'].abs()

    # Choose the fcas values that best matches the scada value during the 5 min interval.
    profile_comp = profile_comp.sort_values('ERROR')
    error_comp = profile_comp.groupby(['SCADA_ELEMENT', 'ELEMENTNUMBER', 'TIMESTAMP'], as_index=False).first()

    # Aggregate the error to comapre each scada and fcas element potential match.
    error_comp = error_comp.groupby(['SCADA_ELEMENT', 'ELEMENTNUMBER'], as_index=False).sum()

    # Sort the comparisons based on aggregate error.
    error_comp = error_comp.sort_values('ERROR')

    # Drop duplicates of element numbers and scada element names, keeping the record for each with the least error.
    best_matches_scada = error_comp[error_comp['SCADAVALUE'].abs() > 0] # Don't include units 0 values for scada
    best_matches_scada = best_matches_scada.drop_duplicates('ELEMENTNUMBER', keep='first')
    best_matches_scada = best_matches_scada.drop_duplicates('SCADA_ELEMENT', keep='first')

    # Remove fcas elements where a match only occurred because both fcas and scada showed no dispatch.
    best_matches_scada['ELEMENTNUMBER'] = pd.to_numeric(best_matches_scada['ELEMENTNUMBER'])
    best_matches_scada = best_matches_scada.sort_values('ELEMENTNUMBER')

    # Give error as a percentage.
    best_matches_scada['ERROR'] = best_matches_scada['ERROR'] / best_matches_scada['SCADAVALUE']

    # drop matches with error greater than 10%
    best_matches_scada = best_matches_scada[(best_matches_scada['ERROR'] < 1) & (best_matches_scada['ERROR'] > -1)]


    return best_matches_scada.loc[:, ('ELEMENTNUMBER', 'SCADA_ELEMENT', 'ERROR')]