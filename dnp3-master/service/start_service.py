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

#########################Edited/Prepared this code by Dr.Venkateswara Reddy Motakatla, NREL (VenkateswaraReddy.Motakatla@nrel.gov)
######################### PNNL Technical support: Poorva, and Alka 

import os
import sys
import time
import csv
import platform
import numpy as np
import yaml
import logging

sys.path.append("../dnp3/service")

from CIMPro_AIAO_BIBO import CIMProcessor

from dnp3.master_pnnl import MyMaster, MyLogger, AppChannelListener, SOEHandler, SOEHandlerSimple, MasterApplication
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
from dnp3.points import PointValue

from gridappsd.topics import simulation_output_topic, simulation_input_topic
from gridappsd import GridAPPSD, DifferenceBuilder, utils


myCIMProcessor = None

_log = logging.getLogger(__name__)


def build_csv_writers(folder, filename, column_names):
    _file = os.path.join(folder, filename)
    if os.path.exists(_file):
        os.remove(_file)
    file_handle = open(_file, 'a')
    csv_writer = csv.writer(file_handle, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(['time'] + column_names)
    return file_handle, csv_writer

def on_message(headers, message):
    #json_msg = yaml.safe_load(str(message))
    #print('Receive message NREL')
    myCIMProcessor.process(message)


def run_master(device_ip_port_config_all_13bus, names,simulation_id,gapps,dnp3_to_cim,conversion_dict):
    masters = []
    for name in names:
        device_ip_port_dict = device_ip_port_config_all_13bus[name]
        HOST=device_ip_port_dict['ip']
        PORT=device_ip_port_dict['port']
        DNP3_ADDR= device_ip_port_dict['link_local_addr']
        LocalAddr= device_ip_port_dict['link_remote_addr']
        convertion_type=device_ip_port_dict['conversion_type']
        object_name=device_ip_port_dict['CIM object']

        application_1 = MyMaster(HOST=HOST,  # "127.0.0.1
                                LOCAL="0.0.0.0",
                                PORT=int(PORT),
                                DNP3_ADDR=int(DNP3_ADDR),
                                LocalAddr=int(LocalAddr),
                                log_handler=MyLogger(),
                                listener=AppChannelListener(),
                                soe_handler=SOEHandler(object_name, convertion_type, dnp3_to_cim,gapps),
                                # soe_handler=SOEHandlerSimple(),
                                master_application=MasterApplication())
        application_1.name=name
        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        print('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))

        masters.append(application_1)

    comID=gapps.subscribe('/topic/goss.gridappsd.field.input', on_message)
    print('Communication ID',comID)
    SLEEP_SECONDS = 1
    time.sleep(SLEEP_SECONDS)
    group_variation = opendnp3.GroupVariationID(32, 2)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 1')
    application_1.master.ScanRange(group_variation, 0, 12)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 2')
    application_1.master.ScanRange(opendnp3.GroupVariationID(32, 2), 0, 3, opendnp3.TaskConfig().Default())
    time.sleep(SLEEP_SECONDS)
    print('\nReading status 3')

    msg_count=0
    csv_dict = {}
    cim_full_msg = {'simulation_id': simulation_id, 'timestamp': str(int(time.time())), 'message':{'measurements':{}}}
    starttime = time.time()
    while True:
        pv_points = []
        current_time = time.time()
        for master in masters:
            global myCIMProcessor
            if 'RTU1' in conversion_dict and 'Analog output' in conversion_dict['RTU1']:
                
                for k, v in conversion_dict['RTU1']['Analog output'].items():
                    
                    if 'CIM mRID' in v:
                        
                        if '_EF7C5ECB-D33A-4087-BEB6-F31D65141B0D' == v['CIM mRID'] and "EnergyConsumer.p" == v['CIM attribute']:                     
                            
                            pv_point = PointValue(command_type=None, function_code=None, value=10, point_def=0, index=int(v['index']), op_type=None) # 2
                        elif '_84344D37-5FF6-41C7-B8BD-0C58C8A6127A' == v['CIM mRID'] and "EnergyConsumer.p" == v['CIM attribute']:
                            pv_point = PointValue(command_type=None, function_code=None, value=15, point_def=0, index=int(v['index']), op_type=None) # 14
                        else :
                            pv_point = PointValue(command_type=None, function_code=None, value=1500, point_def=0, index=int(v['index']), op_type=None) 
           
                        pv_point.measurement_id = v['CIM mRID']
                        pv_point.attribute = v['CIM attribute']
                        pv_points.append(pv_point)        
            if 'RTU1' in conversion_dict and 'Binary output' in conversion_dict['RTU1']:
                
                for k, v in conversion_dict['RTU1']['Binary output'].items():
                    #print('Hi I am at RTU1 stage-2',v)
                    if 'CIM mRID' in v:
                        #print('Hi I am at RTU1 stage-3')
                        if '_7CBC54BB-4A93-410F-AF92-DDA633676AA0' == v['CIM mRID'] and "Switch.open" == v['CIM attribute']:                     
                            #print('Hi I am at RTU1 stage-4')
                            
                            sw_point = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=int(v['index']), op_type=None) # 2
                        elif '_6C1FDA90-1F4E-4716-BC90-1CCB59A6D5A9' == v['CIM mRID'] and "Switch.open" == v['CIM attribute']:
                            sw_point = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=int(v['index']), op_type=None) # 14 

                        sw_point.measurement_id = v['CIM mRID']
                        sw_point.attribute = v['CIM attribute']
                        pv_points.append(sw_point)

            myCIMProcessor = CIMProcessor(pv_points,application_1)
            #myCIMProcessor = CIMProcessor(sw_points,application_1)
            cim_msg = master.soe_handler.get_msg()
            #print('cim_msg',cim_msg)
            #print('cim_msg arrived')
            dnp3_msg_AI = master.soe_handler.get_dnp3_msg_AI()
            #print('Hi I am at RTU1 stage-1',dnp3_msg_AI)
            dnp3_msg_BI = master.soe_handler.get_dnp3_msg_BI()
            if master.name =='RTU1':
                dnp3_msg_AO = myCIMProcessor.get_dnp3_msg_AO()
                dnp3_msg_BO = myCIMProcessor.get_dnp3_msg_BO()
            if master.name+'AI' not in csv_dict:
                rtu_7_csvfile, rtu_7_writer = build_csv_writers('.', master.name+'_AI.csv', list(dnp3_msg_AI.keys()))
                csv_dict[master.name+'AI'] = {'csv_file':rtu_7_csvfile, 'csv_writer':rtu_7_writer}
                csv_dict[master.name+'AI']['csv_writer'].writerow(['time']+master.soe_handler.get_dnp3_msg_AI_header())
            if master.name+'BI' not in csv_dict:                
                rtu_7_csvfile, rtu_7_writer = build_csv_writers('.', master.name+'_BI.csv', list(dnp3_msg_BI.keys()))
                csv_dict[master.name+'BI'] = {'csv_file':rtu_7_csvfile, 'csv_writer':rtu_7_writer}
                # csv_dict[master.name+'BI']['csv_writer'].writerow(['time']+master.soe_handler.get_dnp3_msg_BI_header())
            if master.name =='RTU1':
                if master.name+'AO' not in csv_dict:
                    rtu_7_csvfile, rtu_7_writer = build_csv_writers('.', master.name+'_AO.csv', list(dnp3_msg_AO.keys()))
                    csv_dict[master.name+'AO'] = {'csv_file':rtu_7_csvfile, 'csv_writer':rtu_7_writer}
                    # csv_dict[master.name+'AO']['csv_writer'].writerow(['time']+myCIMProcessor.get_dnp3_msg_AO_header())
                if master.name+'BO' not in csv_dict:
                    rtu_7_csvfile, rtu_7_writer = build_csv_writers('.', master.name+'_BO.csv', list(dnp3_msg_BO.keys()))
                    csv_dict[master.name+'BO'] = {'csv_file':rtu_7_csvfile, 'csv_writer':rtu_7_writer}
                    # csv_dict[master.name+'BO']['csv_writer'].writerow(['time']+myCIMProcessor.get_dnp3_msg_AO_header())
            if master.name =='RTU1':
                if len(dnp3_msg_AO.keys()) > 0:
                    values = [dnp3_msg_AO[k] for k in dnp3_msg_AO.keys()]
                    csv_dict[master.name+'AO']['csv_writer'].writerow(np.insert(values,0, current_time))
                    csv_dict[master.name+'AO']['csv_file'].flush()
                if len(dnp3_msg_BO.keys()) > 0:
                    values = [dnp3_msg_BO[k] for k in dnp3_msg_BO.keys()]
                    csv_dict[master.name+'BO']['csv_writer'].writerow(np.insert(values,0, current_time))
                    csv_dict[master.name+'BO']['csv_file'].flush()

            if master.name =='RTU1':
                if len(dnp3_msg_BI.keys()) > 0:
                    values = [int(dnp3_msg_BI[k]) for k in dnp3_msg_BI.keys()]
                    csv_dict[master.name+'BI']['csv_writer'].writerow(np.insert(values, 0, current_time))
                    csv_dict[master.name+'BI']['csv_file'].flush()
            if len(dnp3_msg_AI.keys()) > 0:
                values = [dnp3_msg_AI[k] for k in dnp3_msg_AI.keys()]
                csv_dict[master.name+'AI']['csv_writer'].writerow(np.insert(values,0, current_time))
                csv_dict[master.name+'AI']['csv_file'].flush()
            else:
                print("no data yet")

            cim_full_msg['message']['measurements'].update(cim_msg)
            cim_full_msg['timestamp'] = str(int(time.time()))
            #print('')
            with open('meas_map.json', 'w') as outfile:
                 json.dump(cim_full_msg, outfile, indent=2)
            print('AI CIM messages',cim_full_msg)
            ###############################     
            gapps.send('/topic/goss.gridappsd.field.output', json.dumps(cim_full_msg))
            master_data = DifferenceBuilder(simulation_id)
            for point in pv_points:
                if point.attribute == "PowerElectronicsConnection.p":
                   master_data.add_difference(point.measurement_id, point.attribute , point.value,point.value)
                   master_open_message = master_data.get_message()
                elif point.attribute == "PowerElectronicsConnection.q":
                   master_data.add_difference(point.measurement_id, point.attribute , point.value,point.value)
                   master_open_message = master_data.get_message()
                elif point.attribute == "EnergyConsumer.p":
                   master_data.add_difference(point.measurement_id, point.attribute , point.value,point.value)
                   master_open_message = master_data.get_message()
                elif point.attribute == "Switch.open":
                   master_data.add_difference(point.measurement_id, point.attribute , point.value,point.value)
                   master_open_message = master_data.get_message()
            
            json_msg = yaml.safe_load(str(master_open_message))  # direct sending of data
            #print('Master send data to outstation',json_msg)
            gapps.send('/topic/goss.gridappsd.field.input', json_msg)
        ########################################################

  

        msg_count+=1
        #print("message count "+ str(msg_count))
        #_log.info("message count "+ str(msg_count))

        #print(master.name+" " +str(cim_full_msg)[:100])
        #_log.info(cim_full_msg)
        time.sleep(2)
        #time.sleep(1.0 - ((time.time() - starttime) % 60.0))


    print('\nStopping')
    for master in masters:
        master.shutdown()
    # application_1.shutdown()
    exit()

    # When terminating, it is necessary to set these to None so that
    # it releases the shared pointer. Otherwise, python will not
    # terminate (and even worse, the normal Ctrl+C won't help).
    application_1.master.Disable()
    application_1 = None
    application_1.channel.Shutdown()
    application_1.channel = None
    application_1.manager.Shutdown()

if __name__ == "__main__":

    import argparse
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("names",  nargs='+', help="name of dnp3 outstation", type=str)
    args = parser.parse_args()
    names = args.names

    
    simulation_id=1234   # Dummy simulation ID
    print('simulation ID created', simulation_id)
    gapps = GridAPPSD(username="system", password="manager")
    gapps.connect()
    
    listening_to_topic = simulation_output_topic(simulation_id)
    with open("config/device_ip_port_config.json") as f:
        device_ip_port_config_all_Xcel = json.load(f)
 

    device_ip_port_dict = device_ip_port_config_all_Xcel[names[0]]
    print(device_ip_port_dict)


    data_loc='config'
    dnp3_to_cim = CIMMapping(conversion_dict=os.path.join(data_loc,"conversion_dict_master_data.json"), model_line_dict=os.path.join(data_loc,"measurement_dict_master.json"))
    conversion_dict = dnp3_to_cim.conversion_dict

    time.sleep(1)

    device_ip_port_dict = device_ip_port_config_all_Xcel[names[0]]
    print(device_ip_port_dict)
    run_master(device_ip_port_config_all_Xcel, names,simulation_id,gapps,dnp3_to_cim,conversion_dict)
    
    
