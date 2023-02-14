
# Copyright (c) 2019 Alliance for Sustainable Energy, LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Edited/Prepared this code by Dr.Venkateswara Reddy Motakatla, NREL

from optparse import Values
from re import A
import pandas as pd
import json
import numpy as np
import csv






def get_device_dict(model_dict, model_line_dict, device_type, name):
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_'+name):
                #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}

                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
    elif device_type == 'RTU':
        for meas in model_dict['feeders'][0]['measurements']:
            #print('meas', meas)
            if meas['name'].startswith('EnergyConsumer_'+name):
                #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
            
            if meas['name'].startswith('LoadBreakSwitch_'+name):
                #             print(meas)
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'pos'}
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}


            if meas['name'].startswith('PowerElectronicsConnection_PhotovoltaicUnit_'+name):
                #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}

            if meas['name'].startswith('PowerElectronicsConnection_BatteryUnit_'+name):
                #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}

    elif device_type == 'Beckwith CapBank':
        # LinearShuntCompensator
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('LinearShuntCompensator_'+name):
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'pos'}
                for cap in model_dict['feeders'][0]["capacitors"]:
                    if cap['name'] == name:
                        model_line_dict[name]['manual close'] = {
                            'mrid': meas['mRID'], 'type': 'magnitude'}
                        print(cap)
                        # TODO figure this out
                        # 0 manual close
                        # 1 manual open
    elif device_type == 'Beckwith LTC':
        for meas in model_dict['feeders'][0]['measurements']:
            # ConductingEquipment_name
            if meas['name'].startswith('RatioTapChanger_'+name):
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {
                        'mrid': meas['mRID'], 'type': 'pos'}
               # for reg in model_dict['feeders'][0]["regulators"]:
                #    if reg['bankName'] == name:
                        # Do I have to count for each position change?
                 #       print(reg)

#--------------------------------------
IEEE123_model='/home/gridapps-d/pydnp3_old/DNP3_NREL_Files/Master_GridAPPSD_files/1514974858_ieee123/model_dict.json'
with open(IEEE123_model) as f:
     model_dict_feeder = json.load(f)
#dict_path='/home/gridapps-d/pydnp3_old/DNP3_NREL_Files/Master_GridAPPSD_files/1514974858_ieee123/model_dict.json'
csv_file=r'IEEE123_RTAC_AI_AO_Mod2.xlsx'
sheet_name='RTU1'
sheet_AO='RTU1_AO'
sheet_BI='RTU1_BI'
sheet_BO='RTU1_BO'
device_type='RTU'
skiprows = 0
if sheet_name == 'RTU1':
    skiprows = 1
rtu_string = sheet_name
master_dict = {}
measurement_mRID_dict = {}


mes_dict = model_dict_feeder['feeders'][0]['measurements']
master_dict['RTU1'] = {'Analog input': {},'Analog output': {},'Binary input': {},'Binary output': {}}

#-------------------------- Analog inputs    
df = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_name)
df = df.replace(np.nan, '', regex=True)

for index, row in df.iterrows():
    temp_dict = row.to_dict()
    index = temp_dict['index']
    master_dict[rtu_string]['Analog input'][index]=temp_dict
    name=temp_dict['CIM name']
    #--------------------------
    
    measurement_mRID_dict[name] = {}
    get_device_dict(model_dict_feeder, measurement_mRID_dict, device_type, name)


#-------------------------- Analog outputs
df1 = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_AO)
df1 = df1.replace(np.nan, '', regex=True)

for index, row in df1.iterrows():
    temp_dict = row.to_dict()
    index = temp_dict['index']
    CIM_name=temp_dict['CIM name']
    
    #-----------------------------------------------------------------
    
    for index1 in range(len(mes_dict)):
        meas_item = mes_dict[index1]
        name_device = meas_item['ConductingEquipment_name']
        mrid_data = meas_item['ConductingEquipment_mRID']
        if name_device==CIM_name:
            mRID={'CIM mRID': mrid_data}
            #master_dict[rtu_string]['Analog output'][index]={'CIM mRID': mrid_data}
    master_dict[rtu_string]['Analog output'][index]={**temp_dict,**mRID}

#-------------------------- Binary inputs
df2 = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_BI)
df2 = df2.replace(np.nan, '', regex=True)

for index, row in df2.iterrows():
    temp_dict = row.to_dict()
    index = temp_dict['index']
    master_dict[rtu_string]['Binary input'][index]=temp_dict
    name=temp_dict['CIM name']
    #--------------------------
    measurement_mRID_dict[name] = {}
    get_device_dict(model_dict_feeder, measurement_mRID_dict, device_type, name)


#-------------------------- Binary outputs
df3 = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_BO)
df3 = df3.replace(np.nan, '', regex=True)


for index, row in df3.iterrows():
    temp_dict = row.to_dict()
    index = temp_dict['index']
    CIM_name=temp_dict['CIM name']
    for index1 in range(len(mes_dict)):
        meas_item = mes_dict[index1]
        name_device = meas_item['ConductingEquipment_name']
        mrid_data = meas_item['ConductingEquipment_mRID']
        if name_device==CIM_name:
            mRID={'CIM mRID': mrid_data}
            #master_dict[rtu_string]['Analog output'][index]={'CIM mRID': mrid_data}
    master_dict[rtu_string]['Binary output'][index]={**temp_dict,**mRID}

print(measurement_mRID_dict)

#---------------creating JSON dict files
with open("measurement_dict_master.json", "w") as f:
        json.dump(measurement_mRID_dict, f, indent=2)

with open("conversion_dict_master_data.json", "w") as f:
        json.dump(master_dict, f, indent=2)
       
   