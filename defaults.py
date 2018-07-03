names = {'DISPATCHLOAD': 'PUBLIC_DVD_DISPATCHLOAD',
         'DUDETAILSUMMARY': 'PUBLIC_DVD_DUDETAILSUMMARY',
         'DISPATCHCONSTRAINT': 'PUBLIC_DVD_DISPATCHCONSTRAINT',
         'GENCONDATA': 'PUBLIC_DVD_GENCONDATA',
         'DISPATCH_UNIT_SCADA': 'PUBLIC_DVD_DISPATCH_UNIT_SCADA',
         'DISPATCHPRICE': 'PUBLIC_DVD_DISPATCHPRICE',
         'SPDREGIONCONSTRAINT': 'PUBLIC_DVD_SPDREGIONCONSTRAINT',
         'SPDCONNECTIONPOINTCONSTRAINT': 'PUBLIC_DVD_SPDCONNECTIONPOINTCONSTRAINT',
         'SPDINTERCONNECTORCONSTRAINT': 'PUBLIC_DVD_SPDINTERCONNECTORCONSTRAINT',
         'BIDPEROFFER_D': 'PUBLIC_DVD_BIDPEROFFER_D',
         'DISPATCHINTERCONNECTORRES': 'PUBLIC_DVD_DISPATCHINTERCONNECTORRES',
         'BIDDAYOFFER_D': 'PUBLIC_DVD_BIDDAYOFFER_D',
         'DISPATCHREGIONSUM': 'PUBLIC_DVD_DISPATCHREGIONSUM',
         'FCAS_4_SECOND': 'FCAS',
         'ELEMENTS_FCAS_4_SECOND': 'Elements_FCAS.csv',
         'VARIABLES_FCAS_4_SECOND': '820-0079 csv.csv',
         'MASTER_REGISTRATION_LIST': 'NEM Registration and Exemption List',
         'FCAS_4s_SCADA_MAP': ''}

return_tables = list(names.keys())

static_tables = ['ELEMENTS_FCAS_4_SECOND', 'VARIABLES_FCAS_4_SECOND', 'MASTER_REGISTRATION_LIST']

static_table_url = {'ELEMENTS_FCAS_4_SECOND': 'https://www.aemo.com.au/-/media/Files/Electricity/NEM/Data/Ancillary_Services/Elements_FCAS.csv',
                    'VARIABLES_FCAS_4_SECOND': 'https://www.aemo.com.au/-/media/Files/CSV/820-0079-csv.csv',
                    'MASTER_REGISTRATION_LIST': 'https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls'}

aemo_data_url = 'http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{}/MMSDM_{}_{}/MMSDM_Historical_Data_SQLLoader/DATA/{}.zip'

fcas_4_url ='http://www.nemweb.com.au/Reports/Current/Causer_Pays/FCAS_{}{}{}{}.zip'

fcas_4_url_hist ='http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/FCAS_Causer_Pays/{}/FCAS_Causer_Pays_{}_{}/FCAS_{}{}{}{}.zip'

data_url = {'DISPATCHLOAD': 'aemo_data_url',
         'DUDETAILSUMMARY': 'aemo_data_url',
         'DISPATCHCONSTRAINT': 'aemo_data_url',
         'GENCONDATA': 'aemo_data_url',
         'STATION': 'aemo_data_url',
         'STADUALLOC': 'aemo_data_url',
         'DISPATCH_UNIT_SCADA': 'aemo_data_url',
         'DUDETAIL': 'aemo_data_url',
         'GENUNITS': 'aemo_data_url',
         'DISPATCHPRICE': 'aemo_data_url',
         'PARTICIPANT': 'aemo_data_url',
         'SPDREGIONCONSTRAINT': 'aemo_data_url',
         'SPDCONNECTIONPOINTCONSTRAINT': 'aemo_data_url',
         'SPDINTERCONNECTORCONSTRAINT': 'aemo_data_url',
         'BIDPEROFFER_D': 'aemo_data_url',
         'DISPATCHINTERCONNECTORRES': 'aemo_data_url',
         'INTERCONNECTOR': 'aemo_data_url',
         'INTERCONNECTORCONSTRAINT': 'aemo_data_url',
         'MNSP_INTERCONNECTOR': 'aemo_data_url',
         'BIDDAYOFFER_D': 'aemo_data_url',
         'DISPATCHREGIONSUM': 'aemo_data_url',
         'MNSP_DAYOFFER': 'aemo_data_url',
         'MNSP_PEROFFER': 'aemo_data_url',
         'LOSSMODEL': 'aemo_data_url',
         'LOSSFACTORMODEL': 'aemo_data_url',
         'DISPATCHCASESOLUTION': 'aemo_data_url',
         'FCAS': 'fcas_4_url'}

filterable_cols = ['DUID', 'REGIONID', 'STATIONID', 'PARTICIPANTID', 'STARTTYPE', 'SCHEDULE_TYPE', 'GENCONID',
                   'BIDTYPE', 'VARIABLEID', 'INTERVENTION', 'DISPATCHMODE', 'STARTTYPE', 'CONNECTIONPOINTID',
                   'DISPATCHTYPE', 'CONSTRAINTID', 'PREDISPATCH', 'STPASA', 'MTPASA', 'LIMITTYPE', 'STATIONNAME',
                   'AGCFLAG', 'INTERCONNECTORID', 'NAME', 'Fuel Source - Primary', 'Fuel Source - Descriptor',
                   'Technology Type - Primary', 'Technology Type - Descriptor']

table_columns = {

    'DISPATCHLOAD': ['SETTLEMENTDATE', 'DUID', 'INTERVENTION', 'DISPATCHMODE', 'AGCSTATUS', 'INITIALMW',
                                  'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE', 'LOWER5MIN', 'LOWER60SEC',
                                  'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC', 'LOWERREG', 'RAISEREG',
                                  'SEMIDISPATCHCAP', 'AVAILABILITY'],

    'DUDETAILSUMMARY': ['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE', 'CONNECTIONPOINTID', 'REGIONID', 'STATIONID',
                        'PARTICIPANTID', 'LASTCHANGED', 'TRANSMISSIONLOSSFACTOR', 'STARTTYPE', 'DISTRIBUTIONLOSSFACTOR',
                        'SCHEDULE_TYPE', 'MAX_RAMP_RATE_UP', 'MAX_RAMP_RATE_DOWN'],

    'DISPATCHCONSTRAINT': ['SETTLEMENTDATE', 'RUNNO', 'CONSTRAINTID', 'INTERVENTION', 'RHS', 'MARGINALVALUE',
                           'VIOLATIONDEGREE', 'LASTCHANGED', 'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO', 'LHS',
                           'DISPATCHINTERVAL'],

    'GENCONDATA': ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE', 'CONSTRAINTVALUE', 'DESCRIPTION',
                   'GENERICCONSTRAINTWEIGHT', 'LASTCHANGED', 'DISPATCH', 'PREDISPATCH', 'STPASA', 'MTPASA',
                   'LIMITTYPE', 'REASON'],

    'STATION': ['STATIONID', 'STATIONNAME', 'LASTCHANGED'],

    'STADUALLOC': ['DUID', 'EFFECTIVEDATE', 'STATIONID', 'VERSIONNO', 'LASTCHANGED'],

    'DISPATCH_UNIT_SCADA': ['SETTLEMENTDATE', 'DUID', 'SCADAVALUE'],

    'DUDETAIL': ['EFFECTIVEDATE', 'DUID', 'VERSIONNO', 'CONNECTIONPOINTID', 'REGISTEREDCAPACITY', 'AGCCAPABILITY',
                 'DISPATCHTYPE', 'MAXCAPACITY', 'STARTTYPE', 'NORMALLYONFLAG', 'LASTCHANGED'],

    'GENUNITS': ['GENSETID', 'STATIONID', 'CDINDICATOR', 'AGCFLAG', 'REGISTEREDCAPACITY', 'DISPATCHTYPE', 'STARTTYPE',
                 'NORMALSTATUS', 'LASTCHANGED', 'CO2E_ENERGY_SOURCE'],

    'DISPATCHPRICE': ['SETTLEMENTDATE', 'REGIONID', 'INTERVENTION', 'RRP', 'RAISE6SECRRP', 'RAISE60SECRRP',
                      'RAISE5MINRRP', 'RAISEREGRRP', 'LOWER6SECRRP', 'LOWER60SECRRP', 'LOWERREGRRP', 'PRICE_STATUS'],

    'PARTICIPANT': ['PARTICIPANTID', 'PARTICIPANTCLASSID', 'NAME', 'LASTCHANGED'],

    'SPDREGIONCONSTRAINT': ['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'FACTOR', 'LASTCHANGED', 'BIDTYPE'],

    'SPDCONNECTIONPOINTCONSTRAINT': ['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'FACTOR', 'BIDTYPE',
                                     'LASTCHANGED'],

    'SPDINTERCONNECTORCONSTRAINT': ['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'FACTOR',
                                     'LASTCHANGED'],

    'BIDPEROFFER_D': ['DUID', 'BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5','BANDAVAIL6',
                      'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9', 'BANDAVAIL10', 'MAXAVAIL', 'RAMPUPRATE',
                      'RAMPDOWNRATE', 'BIDTYPE', 'SETTLEMENTDATE', 'ENABLEMENTMIN', 'ENABLEMENTMAX', 'LOWBREAKPOINT',
                      'HIGHBREAKPOINT', 'INTERVAL_DATETIME'],
    'DISPATCHINTERCONNECTORRES': ['SETTLEMENTDATE', 'INTERCONNECTORID', 'DISPATCHINTERVAL', 'INTERVENTION', 'MWFLOW',
                                  'METEREDMWFLOW'],
    'INTERCONNECTOR': ['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO', 'LASTCHANGED'],
    'INTERCONNECTORCONSTRAINT': ['INTERCONNECTORID', 'FROMREGIONLOSSSHARE', 'EFFECTIVEDATE', 'VERSIONNO',
                                 'LOSSCONSTANT', 'LOSSFLOWCOEFFICIENT', 'ICTYPE'],
    'MNSP_INTERCONNECTOR': ['INTERCONNECTORID', 'LINKID', 'FROMREGION', 'TOREGION', 'MAXCAPACITY', 'FROM_REGION_TLF',
                            'TO_REGION_TLF', 'LHSFACTOR', 'EFFECTIVEDATE', 'VERSIONNO'],
    'BIDDAYOFFER_D': ['SETTLEMENTDATE', 'DUID', 'BIDTYPE', 'OFFERDATE', 'VERSIONNO', 'PRICEBAND1', 'PRICEBAND2',
                      'PRICEBAND3',  'PRICEBAND4',  'PRICEBAND5',  'PRICEBAND6',  'PRICEBAND7',  'PRICEBAND8',
                      'PRICEBAND9',  'PRICEBAND10', 'T1', 'T2', 'T3', 'T4'],
    'DISPATCHREGIONSUM': ['SETTLEMENTDATE', 'REGIONID', 'DISPATCHINTERVAL', 'INTERVENTION', 'TOTALDEMAND',
                          'AVAILABLEGENERATION', 'AVAILABLELOAD', 'DEMANDFORECAST', 'DISPATCHABLEGENERATION',
                          'DISPATCHABLELOAD', 'NETINTERCHANGE', 'EXCESSGENERATION', 'LOWER5MINLOCALDISPATCH',
                          'LOWER60SECLOCALDISPATCH', 'LOWER6SECLOCALDISPATCH', 'RAISE5MINLOCALDISPATCH',
                          'RAISE60SECLOCALDISPATCH', 'RAISE6SECLOCALDISPATCH', 'LOWERREGLOCALDISPATCH',
                          'RAISEREGLOCALDISPATCH', 'INITIALSUPPLY', 'CLEAREDSUPPLY', 'TOTALINTERMITTENTGENERATION',
                          'DEMAND_AND_NONSCHEDGEN', 'UIGF', 'SEMISCHEDULE_CLEAREDMW', 'SEMISCHEDULE_COMPLIANCEMW'],
    'MNSP_PEROFFER': ['SETTLEMENTDATE', 'OFFERDATE', 'VERSIONNO', 'PARTICIPANTID', 'LINKID', 'PERIODID',
                      'BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5', 'BANDAVAIL6',
                      'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9', 'BANDAVAIL10'],
    'MNSP_DAYOFFER': ['SETTLEMENTDATE', 'OFFERDATE', 'VERSIONNO', 'PARTICIPANTID', 'LINKID', 'PERIODID',
                      'PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3',  'PRICEBAND4',  'PRICEBAND5',  'PRICEBAND6',
                      'PRICEBAND7',  'PRICEBAND8', 'PRICEBAND9',  'PRICEBAND10'],
    'LOSSMODEL': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'LOSSSEGMENT', 'MWBREAKPOINT'],
    'LOSSFACTORMODEL': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'REGIONID', 'DEMANDCOEFFICIENT'],
    'DISPATCHCASESOLUTION': ['SETTLEMENTDATE', 'TOTALOBJECTIVE'],
    'FCAS_4_SECOND': ['TIMESTAMP', 'ELEMENTNUMBER', 'VARIABLENUMBER', 'VALUE', 'VALUEQUALITY'],
    'ELEMENTS_FCAS_4_SECOND': ['ELEMENTNUMBER', 'ELEMENTNAME', 'ELEMENTTYPE', 'NAME'],
    'VARIABLES_FCAS_4_SECOND': ['VARIABLENUMBER', 'VARIABLETYPE'],
    'MASTER_REGISTRATION_LIST': ['Participant', 'Station Name' ,'Region' ,'Dispatch Type', 'Category', 'Classification',
         'Fuel Source - Primary', 'Fuel Source - Descriptor', 'Technology Type - Primary',
         'Technology Type - Descriptor' , 'Aggregation', 'DUID'],
    'FCAS_4s_SCADA_MAP': ['ELEMENTNUMBER', 'MARKETNAME']}

table_primary_keys = {'DISPATCHCONSTRAINT': ['CONSTRAINTID', 'EFFECTIVEDATE', 'VERSIONNO'],
                      'DUDETAILSUMMARY': ['DUID', 'START_DATE'], 'STATION': ['STATIONID'],
                      'STADUALLOC': ['EFFECTIVEDATE', 'STATIONID', 'VERSIONNO'],
                      'GENUNITS': ['GENSETID'],
                      'PARTICIPANT': ['PARTICIPANTID'],
                      'SPDREGIONCONSTRAINT': ['EFFECTIVEDATE', 'GENCONID','REGIONID', 'VERSIONNO', 'BIDTYPE'],
                      'SPDCONNECTIONPOINTCONSTRAINT': ['EFFECTIVEDATE', 'GENCONID','CONNECTIONPOINTID', 'VERSIONNO',
                                                       'BIDTYPE'],
                      'SPDINTERCONNECTORCONSTRAINT': ['EFFECTIVEDATE','GENCONID','INTERCONNECTORID', 'VERSIONNO'],
                      'GENCONDATA': ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'],
                      'MNSP_PEROFFER': ['SETTLEMENTDATE', 'OFFERDATE', 'VERSIONNO', 'PARTICIPANTID', 'LINKID'],
                      'MNSP_DAYOFFER': ['SETTLEMENTDATE', 'OFFERDATE', 'VERSIONNO', 'PARTICIPANTID', 'LINKID'],
                      'INTERCONNECTORCONSTRAINT': ['EFFECTIVEDATE', 'INTERCONNECTORID', 'VERSIONNO'],
                      'MNSP_INTERCONNECTOR': ['EFFECTIVEDATE', 'LINKID', 'VERSIONNO'],
                      'LOSSMODEL': ['EFFECTIVEDATE', 'INTERCONNECTORID', 'LOSSSEGMENT', 'VERSIONNO'],
                      'LOSSFACTORMODEL': ['EFFECTIVEDATE', 'INTERCONNECTORID', 'REGIONID', 'VERSIONNO'],
                      'BIDPEROFFER_D': ['BIDTYPE', 'DUID', 'OFFERDATE', 'PERIODID', 'SETTLEMENTDATE'],
                      'DISPATCHINTERCONNECTORRES': ['DISPATCHINTERVAL', 'INTERCONNECTORID', 'INTERVENTION',
                                                    'SETTLEMENTDATE'],
                      'INTERCONNECTOR': ['INTERCONNECTORID'],
                      'DISPATCHPRICE': ['DISPATCHINTERVAL', 'INTERVENTION', 'REGIONID', 'SETTLEMENTDATE'],
                      'BIDDAYOFFER_D': ['BIDTYPE', 'DUID', 'SETTLEMENTDATE'],
                      'DISPATCHREGIONSUM': ['DISPATCHINTERVAL', 'INTERVENTION', 'REGIONID', 'SETTLEMENTDATE']}

effective_date_group_col = {'SPDREGIONCONSTRAINT': ['GENCONID'],
                            'SPDCONNECTIONPOINTCONSTRAINT': ['GENCONID'],
                            'SPDINTERCONNECTORCONSTRAINT': ['GENCONID'],
                            'GENCONDATA': ['GENCONID'],
                            'STADUALLOC': ['STATIONID'],
                            'MNSP_INTERCONNECTOR': ['INTERCONNECTORID'],
                            'INTERCONNECTORCONSTRAINT': ['INTERCONNECTORID'],
                            'INTERCONNECTOR': ['INTERCONNECTORID'],
                            'LOSSMODEL': ['INTERCONNECTORID'],
                            'LOSSFACTORMODEL': ['INTERCONNECTORID'],
                            'DUDETAILSUMMARY': ['DUID'],
                            'MNSP_PEROFFER': ['LINKID'],
                            'MNSP_DAYOFFER': ['LINKID']}


months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

nem_data_model_start_time = '2009/07/01 00:00:00'

fcas_start_index = 1
fcas_end_index = 2355

header_y_pad = 30
query_y_pad = (20, 0)
query_row_offset = 2
row_height = 6
names_internal_row = 1
table_list_internal_row = 1
start_time_label_internal_row = 2
start_time_internal_row = 3
end_time_label_internal_row = 4
end_time_internal_row = 5
plus_internal_row = 6
plus_merge_internal_row = 7
list_row_span = 5
list_column_span = 2
save_field_column_span = 3
standard_x_pad = (0, 10)
list_filter_row_span = 4
internal_filter_row = 2
delete_button_internal_row = 5
last_column = 100
join_type = ['inner', 'left', 'right']
