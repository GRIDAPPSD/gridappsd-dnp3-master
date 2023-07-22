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

import argparse
import json
import os
import sys
import time
import csv
import platform
import numpy as np
import yaml
import logging

from dnp3.CIMPro_AIAO_BIBO import CIMProcessor

from dnp3.master_pnnl import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
from dnp3.points import PointValue

from gridappsd.topics import simulation_output_topic, simulation_input_topic
from gridappsd import GridAPPSD, DifferenceBuilder, utils

myCIMProcessor = None

logging.basicConfig(level=logging.DEBUG)
_log = logging.getLogger(__name__)

def on_message(headers, message):
    myCIMProcessor.process(message)


def run_master(device_ip_port_config_all_13bus, names,simulation_id,gapps,dnp3_to_cim,conversion_dict):
    masters = []
    
    #print("June",device_ip_port_config_all_13bus, names,simulation_id,gapps,dnp3_to_cim,conversion_dict)
    for name in names:
        device_ip_port_dict = device_ip_port_config_all_13bus[name]
        HOST=device_ip_port_dict['ip']
        PORT=device_ip_port_dict['port']
        DNP3_ADDR= device_ip_port_dict['link_local_addr']
        LocalAddr= device_ip_port_dict['link_remote_addr']
        convertion_type=device_ip_port_dict[
            'conversion_type']
        object_name=device_ip_port_dict['CIM object']
     
        application_1 = MyMaster(HOST=HOST,  # "127.0.0.1
                                LOCAL="0.0.0.0",
                                PORT=int(PORT),
                                DNP3_ADDR=int(DNP3_ADDR),
                                LocalAddr=int(LocalAddr),
                                log_handler=MyLogger(),
                                listener=AppChannelListener(),
                                soe_handler=SOEHandler(object_name, convertion_type, dnp3_to_cim,gapps),
                                master_application=MasterApplication())
        application_1.name=name
        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        _log.debug('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))

        masters.append(application_1)
        
    pv_points = []
    for key in conversion_dict:
        global myCIMProcessor
        for outstation_name in conversion_dict:
            for key in conversion_dict[outstation_name]:
                if key == 'Analog output':
                    for index, v in conversion_dict[outstation_name][key].items():
                        pv_point = PointValue(command_type=None, function_code=None, value=1500, point_def=0, index=int(v['index']), op_type=None) 
                        pv_point.measurement_id = v['CIM mRID']
                        pv_point.attribute = v['CIM attribute']
                        pv_points.append(pv_point) 
                elif key == 'Binary output':
                    for index, v in conversion_dict[outstation_name][key].items():
                        pv_point = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=int(v['index']), op_type=None) # 2
                        pv_point.measurement_id = v['CIM mRID']
                        pv_point.attribute = v['CIM attribute']
                        pv_points.append(pv_point)
  
    myCIMProcessor = CIMProcessor(pv_points,application_1)  
        
    gapps.subscribe('/topic/goss.gridappsd.field.input', on_message)

    SLEEP_SECONDS = 1
    time.sleep(SLEEP_SECONDS)
    group_variation = opendnp3.GroupVariationID(32, 2)
    application_1.master.ScanRange(group_variation, 0, 12)
    application_1.master.ScanRange(opendnp3.GroupVariationID(32, 2), 0, 3, opendnp3.TaskConfig().Default())
    time.sleep(SLEEP_SECONDS)

    msg_count=0
    csv_dict = {}
    cim_full_msg = {'simulation_id': simulation_id, 'message':{'timestamp': int(time.time()),'measurements':{}}}
    while True:
        current_time = time.time()
        for master in masters:
            cim_msg = master.soe_handler.get_msg()
            cim_full_msg['message']['measurements'].update(cim_msg)
            cim_full_msg['message']['timestamp'] = str(int(current_time))
            _log.debug(f'Publishing CIM measurement: {json.dumps(cim_full_msg)}')
            gapps.send('/topic/goss.gridappsd.field.output', json.dumps(cim_full_msg))
        
        msg_count+=1
        time.sleep(2)

    _log.info('\n Stopping DNP3 Master Service')
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

    parser = argparse.ArgumentParser()
    parser.add_argument("names",  nargs='+', help="name of dnp3 outstation", type=str)
    parser.add_argument("--config_path",
                        help="Path of the folder containing input configuration files",
                        default='config',
                        type=str)
    args = parser.parse_args()
    outstation_names = args.names
    config_path = args.config_path

    #TODO: Change dummy simulation id to field id
    simulation_id='field_data'   
    gapps = GridAPPSD(username="system", password="manager")
    gapps.connect()
    
    with open(config_path+"/device_ip_port_config.json") as f:
        device_ip_port_config_all_Xcel = json.load(f)
 
    dnp3_to_cim = CIMMapping(conversion_dict=os.path.join(config_path,"conversion_dict_master.json"), model_line_dict=os.path.join(config_path,"model_line_dict.json"))
    conversion_dict = dnp3_to_cim.conversion_dict
    #print(conversion_dict)

    time.sleep(1)

    run_master(device_ip_port_config_all_Xcel, outstation_names,simulation_id,gapps,dnp3_to_cim,conversion_dict)
    
    
