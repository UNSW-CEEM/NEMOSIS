from nemosis import dynamic_data_compiler
import plotly.express as px

start_time = '2021/05/01 11:00:00'
end_time = '2021/05/01 12:00:00'
raw_data_cache = 'C:/Users/nick/Desktop/cache'

scada = dynamic_data_compiler(start_time, end_time, 'FCAS_4_SECOND', raw_data_cache,
                              filter_cols=['ELEMENTNUMBER', 'VARIABLENUMBER'],
                              filter_values=([330, 331], [2, 3, 4, 5]), fformat='parquet')

elements = {
    330: 'HPRG',
    331: 'HPRL'
}

variables = {
    2: 'scada_value',
    3: 'dispatch_target',
    5: 'regulation_target'
}

scada['descriptor'] = (scada['ELEMENTNUMBER'].apply(lambda x: elements[x]) + '_' +
                       scada['VARIABLENUMBER'].apply(lambda x: variables[x]))

scada = scada.pivot(index='TIMESTAMP', columns='descriptor', values='VALUE')

scada['interval_end_time'] = scada.index.dt.round('5min')

scada['time_left_in_interval'] = scada['interval_end_time'] - scada.index

scada['']

scada['target'] = (scada['HPRG_dispatch_target'] + scada['HPRG_regulation_target'] -
                   scada['HPRL_dispatch_target'] - scada['HPRL_regulation_target'])

scada['scada_value'] = (scada['HPRG_scada_value'] - scada['HPRL_scada_value'])

fig = px.line(scada, x=scada.index, y=['target', 'scada_value'])
fig.show()


