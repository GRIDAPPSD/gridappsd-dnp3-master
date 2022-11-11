import json
import pandas as pd
import numpy as np

def get_conversion_model(csv_file,sheet_name):
    df = pd.read_excel(csv_file,sheet_name=sheet_name)
    df = df.replace(np.nan, '', regex=True)
    master_dict ={
        'Analog input':{},
        'Analog output':{},
        'Binary input':{},
        'Binary output':{} }
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
        #print(type_name)
        next_value = next(it)
        #print (x+1, next_value)
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

def build_conversion(csv_file):
    conversion_dict = {}
    shark = get_conversion_model(csv_file, sheet_name='Shark')
    conversion_dict['Shark'] = shark
    shark = get_conversion_model(csv_file, sheet_name='Beckwith CapBank 2')
    conversion_dict['Beckwith CapBank'] = shark
    shark = get_conversion_model(csv_file, sheet_name='Beckwith LTC')
    conversion_dict['Beckwith LTC'] = shark
    with open("conversion_dict.json", "w") as f:
        json.dump(conversion_dict, f, indent=2)

# def build_conversion(DNP3_device_xlsx):
#     df = pd.read_excel(r'DNP3 list.xlsx', sheet_name='Shark')
#     conversion_dict = {}
#     df = df.set_index('Index')
#     shark_dict = df.T.to_dict()
#     conversion_dict['Shark'] = shark_dict
#     conversion_dict = {"Shark": shark_dict}
#     with open("conversion_dict.json", "w") as f:
#         json.dump(conversion_dict, f, indent=2)

def get_device_dict(model_dict, model_line_dict, device_type, name):
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_'+name):
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
                        #print(cap)
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

def model_line_dict():
    with open("model_dict.json") as f:
        model_dict = json.load(f)

    line_name = "632633"
    cap_name = "cap1"
    reg_name = "Reg"
    model_line_dict = {line_name: {},
                       cap_name: {},
                       reg_name: {}}

    name = line_name
    device_type = "Shark"
    get_device_dict(model_dict, model_line_dict, device_type, name)
    device_type = "Beckwith CapBank 2"
    name = "cap1"
    get_device_dict(model_dict, model_line_dict, device_type, name)
    device_type = 'Beckwith LTC'
    name = "Reg"
    get_device_dict(model_dict, model_line_dict, device_type, name)

    with open("model_line_dict.json", "w") as f:
        json.dump(model_line_dict, f, indent=2)

def model_line_dict_old(model_dict_json):
    from_node = '632'
    to_node = '633'
    node_name = '633'
    line_name = from_node + to_node
    line_name = from_node + to_node
    cap_name = "cap1"
    reg_name = "Reg"

    with open("model_dict.json") as f:
        model_dict = json.load(f)

    model_line_dict = {line_name: {},
                       cap_name: {}}
    device_type = "Shark"
    device_type = "Beckwith CapBank 2"
    device_type = 'Beckwith LTC'
    # if device_type == 'Shark':
    #     for meas in model_dict['feeders'][0]['measurements']:
    #         if meas['name'].startswith('ACLineSegment_' + line_name):
    #             print(meas)
    #             if meas['measurementType'] == 'PNV':
    #                 model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-N)'] = {
    #                     'mrid': meas['mRID'], 'type': 'magnitude'}
    #                 model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-L)'] = {
    #                     'mrid': meas['mRID'], 'type': 'angle'}
    #             if meas['measurementType'] == 'VA':
    #                 model_line_dict[from_node + to_node]['P ' + meas['phases']] = {'mrid': meas['mRID'],
    #                                                                                'type': 'magnitude'}
    #                 model_line_dict[from_node + to_node]['Q ' + meas['phases']] = {'mrid': meas['mRID'],
    #                                                                                'type': 'angle'}
    #     #     if meas['ConnectivityNode'] == node_name and meas['ConductingEquipment_type'] == 'ACLineSegment':
    #     #         print(meas)
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_' + line_name):
                #print(meas)
                if meas['measurementType'] == 'PNV':
                    model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-N)'] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-L)'] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    model_line_dict[from_node + to_node]['P ' + meas['phases']] = {'mrid': meas['mRID'],
                                                                                   'type': 'magnitude'}
                    model_line_dict[from_node + to_node]['Q ' + meas['phases']] = {'mrid': meas['mRID'],
                                                                                   'type': 'angle'}
    elif device_type == 'Beckwith CapBank 2':
        # LinearShuntCompensator
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('LinearShuntCompensator_' + cap_name):
                if meas['measurementType'] == 'PNV':
                    #                     print(meas)
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'],
                                                                               'type': 'magnitude'}
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'VA':
                    #                     print(meas)
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'],
                                                                               'type': 'magnitude'}
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'POS':
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'pos'}
                    pass
        for cap in model_dict['feeders'][0]["capacitors"]:
            if cap['name'] == cap_name:
                model_line_dict[cap_name]['manual close'] = {'mrid': meas['mRID'], 'type': 'magnitude'}
                #print(cap)
                # TODO figure this out
                # 0 manual close
                # 1 manual open
    elif device_type == 'Beckwith LTC':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('LinearShuntCompensator_' + cap_name):
                if meas['measurementType'] == 'PNV':
                    #                     print(meas)
                    model_line_dict[reg_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'],
                                                                               'type': 'magnitude'}
                    model_line_dict[reg_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'VA':
                    #                     print(meas)
                    model_line_dict[reg_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'],
                                                                               'type': 'magnitude'}
                    model_line_dict[reg_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'POS':
                    # Tap position
                    model_line_dict[cap_name]['? ' + meas['phases'] + ' ?'] = {'mrid': meas['mRID'], 'type': 'pos'}
                    pass
        for reg in model_dict['feeders'][0]["regulators"]:
            if reg['bankName'] == reg_name:
                # Do I have to count for each position change?
                print(reg)

    model_line_dict
    with open("model_line_dict.json", "w") as f:
        json.dump(model_line_dict, f, indent=2)

class CIMMapping():
    """ This creates dnp3 input and output points for incoming CIM messages  and model dictionary file respectively."""

    def __init__(self, conversion_dict="conversion_dict.json", model_line_dict="model_line_dict.json"):
        with open(conversion_dict) as f:
            #     json.dump(shark_dict,f)
            conversion_dict = json.load(f)
        self.conversion_dict = conversion_dict

        with open(model_line_dict) as f:
            model_line_dict = json.load(f)
        self.model_line_dict = model_line_dict

if __name__ == '__main__':
    CIMMapping()
