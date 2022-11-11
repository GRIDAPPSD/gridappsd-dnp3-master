
from optparse import Values
from re import A
import pandas as pd
import json
import numpy as np
import csv



with open('/home/gridapps-d/pydnp3_old/gridappsd-dnp3-NREL_use_case_3/Use_case_6_Excel/993911893/model_dict.json', 'r') as f:
     name_mrid= json.load(f)

guess={}
yes=name_mrid['feeders'][0]['measurements']

for index1 in range(len(yes)): 
     meas_item = yes[index1]
     name_device=meas_item['ConductingEquipment_name']
     mrid_data=meas_item['ConductingEquipment_mRID']
     guess[name_device]={'name':name_device,'mRID':mrid_data}

with open('guess_mrid.json', 'w') as json_file:
     json.dump(guess, json_file)



guess={}
yes=name_mrid['feeders'][0]['solarpanels']

for index1 in range(len(yes)): 
     meas_item = yes[index1]
     name_device=meas_item['name']
     mrid_data=meas_item['mRID']
     guess[name_device]={'name':name_device,'mRID':mrid_data}

yes=name_mrid['feeders'][0]['batteries']

for index1 in range(len(yes)): 
     meas_item = yes[index1]
     name_device=meas_item['name']
     mrid_data=meas_item['mRID']
     guess[name_device]={'name':name_device,'mRID':mrid_data}


#guss_total=guess+guess1

with open('guess_mrid_1.json', 'w') as json_file:
     json.dump(guess, json_file)


                

def get_conversion_model(csv_file,sheet_name):
    df = pd.read_excel(csv_file,sheet_name=sheet_name)
    df = df.replace(np.nan, '', regex=True)
    master_dict ={
        'Analog input':{},
        'Analog output':{},
        'Binary input':{},
        'Binary output':{}  }

#     temp_df = pd.DataFrame(df[1:4])
#     temp_df = temp_df.set_index('Index')
#     master_dict['Analog input'] = temp_df.T

#     temp_df = pd.DataFrame(df[6:10])
#     temp_df = temp_df.set_index('Index')
#     master_dict['Analog output'] = temp_df.T

    x = []

    for index, row in df.iterrows():
#         print(pd.isna(row['Multiplier']))
        if pd.isna(row['Multiplier']) or row['Multiplier'] == '':
#             print(index)
            x.append(index)
    #print(df.shape)
    x.append(df.shape[0])
    it = iter(x)
    for x in it:
#         print(x)
        type_name = df.iloc[x][1]
        #(type_name)
        next_value = next(it)
       # print (x+1, next_value)
        #print(pd.DataFrame(df[x+1:next_value]))
        temp_df = pd.DataFrame(df[x+1: next_value])
#         temp_df = temp_df.set_index('Index')
#         temp_df = temp_df.replace(np.nan, '', regex=True)
        temp_dict = temp_df.T.to_dict()
        temp_dict_set_key_to_index = {}
        for k,v in temp_dict.items():
            temp_dict_set_key_to_index[int(v['Index'])] = v
        master_dict[type_name] = temp_dict_set_key_to_index
    return master_dict


def convert_rtu(csv_file=r'UC6_RTAC_update.xlsx', sheet_name='RTU1',sheet_name1='RTU1_AO'):
    skiprows = 0
    if sheet_name == 'RTU1':
        skiprows = 1
    df = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_name)
    df = df.replace(np.nan, '', regex=True)

    df1 = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_name1)
    df1 = df1.replace(np.nan, '', regex=True)

    master_dict = {}
    master_dict['RTU1'] = {'Analog input': {},'Analog output': {}}
    master_dict1={}
    conversion_name_dict = {}
    phase_dict = {'1': 'A', '2': 'B', '3': 'C'}
    for index, row in df.iterrows():
        rtu_string = sheet_name
       # if rtu_string not in master_dict:
        #    master_dict[rtu_string] = {'Analog input': {},'Analog output': {}}
        #             conversion_name_dict[rtu_string] = []
        temp_dict = row.to_dict()
        index = temp_dict['Index']
        temp_dict['Index'] = index
        temp_name = temp_dict['Name']
        if not temp_name.strip():
            print('empty')
            break
        if 'LTC_' in temp_name:
            processed_name = temp_name + '_A'  # fake phase added I don't know what to do with this
        else:
            #last_underscore_index = temp_name.rindex('_')
            #processed_name = temp_name[:last_underscore_index]
            processed_name = temp_name

        temp_type = processed_name[0]

        temp_phase = processed_name[-1]

        type_mes=processed_name[3]

       # print('temp_phase',processed_name)
        processes_dict = {}
        #         break
        processes_dict['orig_name'] = temp_name
        processes_dict['index'] = index
        processes_dict['Multiplier'] = 1
        if temp_phase =='1':
           processes_dict['CIM phase'] = 'A'
        elif temp_phase =='2' :
           processes_dict['CIM phase'] = 'B'
        elif temp_phase =='3':
           processes_dict['CIM phase'] = 'C'
        processes_dict['CIM attribute'] = 'magnitude'

        if type_mes =='p':
           processes_dict['CIM units'] = 'W'
           processes_dict['CIM Variable'] = 'P'
           processes_dict['CIM type'] = 'VA'
        elif type_mes =='q':
           processes_dict['CIM units'] = 'Var'
           processes_dict['CIM Variable'] = 'Q'
           processes_dict['CIM type'] = 'VA'
        elif type_mes == 'v':
           processes_dict['CIM units'] = 'PNV'
           processes_dict['CIM Variable'] = 'V'
           processes_dict['CIM type'] = 'PNV'



        processes_dict['CIM name'] = processed_name[6:].lower()  # Hope this is good :)
        #         print(temp_dict)
        #         break
        name_phase = processed_name
        if name_phase in conversion_name_dict:
            print('duplicate name', name_phase)
        else:
            conversion_name_dict[name_phase] = processes_dict
        if temp_type == 'V':
            processes_dict['CIM units'] = 'PNV'

        master_dict[rtu_string]['Analog input'][index] = processes_dict
    ###################################################################       
    for index_AO, row in df1.iterrows():
        #rtu_string1 = sheet_name1
        temp_dict1 = row.to_dict()
        index_AO = temp_dict1['Index']
        temp_dict1['Index'] = index_AO
        temp_name1 = temp_dict1['Name']
            #print('temp_name1',temp_name1)
        processed_name_AO = temp_name1
        att='PowerElectronicsConnection'+'.'+processed_name_AO[3]
            #print(att)
            ####################################################################################
           # with open('/home/gridapps-d/pydnp3_old/gridappsd-dnp3-NREL_use_case_3/Use_case_6_Excel/name_mrid.csv','rt') as csv_file:
            #     data = csv.reader(csv_file)
             #    name_mrid=[row for row in data]
        processes_dict_AO = {}
        processes_dict_AO['orig_name'] = temp_name1
        processes_dict_AO['index'] = index_AO
        processes_dict_AO['Multiplier'] = 1
        if temp_phase =='1':
            processes_dict_AO['CIM phase'] = 'A'
        elif temp_phase =='2' :
            processes_dict_AO['CIM phase'] = 'B'
        elif temp_phase =='3':
            processes_dict_AO['CIM phase'] = 'C'
        processes_dict_AO['CIM attribute'] = att
        processes_dict_AO['CIM units'] = 'VA'
        processes_dict_AO['CIM name'] = processed_name_AO[6:].lower()  # Hope this is good :)

        with open('/home/gridapps-d/pydnp3_old/gridappsd-dnp3-NREL_use_case_3/Use_case_6_Excel/guess_mrid_1.json', 'r') as f:
            guess_mrid= json.load(f)

        name=processed_name_AO[6:].lower()
        if name in guess_mrid.keys():
            processes_dict_AO['CIM mRID']=guess_mrid[name]['mRID']

        name_phase_AO = processed_name_AO
        conversion_name_dict[name_phase_AO] = processes_dict_AO     
        master_dict[rtu_string]['Analog output'][index_AO] = processes_dict_AO
            
    return master_dict, conversion_name_dict



def get_device_dict(model_dict, model_line_dict, device_type, name):
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}

                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
    elif device_type == 'RTU':
        for meas in model_dict['feeders'][0]['measurements']:
            #print('meas', meas)
            if meas['name'].startswith('EnergyConsumer_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}

            if meas['name'].startswith('PowerElectronicsConnection_PhotovoltaicUnit_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}

            if meas['name'].startswith('PowerElectronicsConnection_BatteryUnit_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}

            

    elif device_type == 'Beckwith CapBank':
    #LinearShuntCompensator
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('LinearShuntCompensator_'+name):
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']]  = {'mrid':meas['mRID'],'type':'pos'}
                for cap in model_dict['feeders'][0]["capacitors"]:
                    if cap['name'] == name:
                        model_line_dict[name]['manual close'] = {'mrid':meas['mRID'],'type':'magnitude'}
                        print(cap)
                        #TODO figure this out
                        # 0 manual close
                        # 1 manual open
    elif device_type == 'Beckwith LTC':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('RatioTapChanger_'+name):#ConductingEquipment_name
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']]  = {'mrid':meas['mRID'],'type':'pos'}
                for reg in model_dict['feeders'][0]["regulators"]:
                    if reg['bankName'] == name:
                        # Do I have to count for each position change?
                        print(reg)

def build_RTAC(filename_rtu=r'UC6_RTAC_update.xlsx',
               filename_eq='UC6_DNP3 list.xlsx',
               Xcel_model=''):
    conversion_dict_master = {}
    conversion_name_dict_master = {}

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU1',sheet_name1='RTU1_AO')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)
    #print('conversion_name_dict_master',conversion_name_dict_master)


    ## individual equipment conversion parts, saves to file
    build_eq_conversion_dict(filename_eq)

    # Load EQ model
    with open('conversion_dict_eq.json') as json_file:
        data = json.load(json_file)

    data.update(conversion_dict_master)

    with open("conversion_dict_master_1.json", "w") as f:
        json.dump(data, f, indent=2)
    conversion_dict = data

    with open(Xcel_model) as f:
        model_dict = json.load(f)

    model_line_dict = {}
    ################################## Capacitor
    #device_type = "Beckwith CapBank"
    #name = "Capacitor.3-2120-535-136-058_233786693 "
    #model_line_dict[name] = {}
    #get_device_dict(model_dict, model_line_dict, device_type, name)
    ################################## LTC
    #device_type = 'Beckwith LTC'
    #name = "s1"
    #model_line_dict[name] = {}
    #get_device_dict(model_dict, model_line_dict, device_type, name)

    for full_name, name_dict in conversion_name_dict_master.items():
        name = name_dict['CIM name']
        device_type = "RTU"
        #         name='tran_xf_701_1095507_5022'
        model_line_dict[name] = {}
        get_device_dict(model_dict, model_line_dict, device_type, name)

    with open("model_line_dict_master_1.json", "w") as f:
        json.dump(model_line_dict, f, indent=2)


def build_eq_conversion_dict(csv_file = 'UC6_DNP3 list.xlsx'):
    conversion_dict_eq = {}
    # csv_file = 'DNP3 list.xlsx'
    shark = get_conversion_model(csv_file, sheet_name='Shark')
    conversion_dict_eq['Shark'] = shark
    sheet_name = 'Beckwith CapBank 2'
    beckwith_capbank = get_conversion_model(csv_file, sheet_name)
    conversion_dict_eq['Beckwith CapBank'] = beckwith_capbank
    sheet_name = 'Beckwith LTC'
    beckwith_capbank = get_conversion_model(csv_file, sheet_name)
    conversion_dict_eq[sheet_name] = beckwith_capbank
    #######################################################################
    sheet_name = 'RTU1'
    beckwith_capbank = get_conversion_model(csv_file, sheet_name)
    conversion_dict_eq[sheet_name] = beckwith_capbank
    
    with open("conversion_dict_eq_1.json", "w") as f:
        json.dump(conversion_dict_eq, f, indent=2)


if __name__ == '__main__':
    build_RTAC(filename_rtu=r'UC6_RTAC_update.xlsx',
               filename_eq='UC6_DNP3 list.xlsx',
               Xcel_model = '/home/gridapps-d/pydnp3_old/gridappsd-dnp3-NREL_use_case_3/Use_case_6_Excel/993911893/model_dict.json')
