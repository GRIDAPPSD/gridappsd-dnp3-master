import os
import sys
import time
import csv
import platform
import numpy as np
import yaml
import logging

sys.path.append("../dnp3/service")

from CIMProcessor import CIMProcessor

from dnp3.master import MyMaster, MyLogger, AppChannelListener, SOEHandler, SOEHandlerSimple, MasterApplication
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
from dnp3.points import PointValue

from gridappsd.topics import simulation_output_topic, simulation_input_topic
from gridappsd import GridAPPSD, DifferenceBuilder, utils
import Simulation_ID_GridLABD     # Simulation ID and settings

#myCIMProcessor = None

_log = logging.getLogger(__name__)


#simid=1852861267
def build_csv_writers(folder, filename, column_names):
    _file = os.path.join(folder, filename)
    if os.path.exists(_file):
        os.remove(_file)
    file_handle = open(_file, 'a')
    csv_writer = csv.writer(file_handle, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    # csv_writer.writerow(column_names)
    csv_writer.writerow(['time'] + column_names)
    return file_handle, csv_writer

def on_message(headers, message):
    json_msg = yaml.safe_load(str(message))
    print('Receive message')
    #print('HI NREL',json_msg)
    # _log.info("Receive message")
    #_log.info(json_msg)
    # find point to master list
   # print('Receive message NREL',json_msg)

   # print('Receive message NREL',message['message']['measurements'])

    myCIMProcessor.process(json_msg)

# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_master(device_ip_port_config_all_13bus, names,simulation_id):
    #export GRIDAPPSD_USER=system
    #export GRIDAPPSD_PASSWORD=manager
    #gapps = GridAPPSD(2111478722, address=utils.get_gridappsd_address(),
    #                  username=utils.get_gridappsd_user(), #password=utils.get_gridappsd_pass())
    

    # Establish connection to GridAPPS-D Platform:
    from gridappsd import GridAPPSD
    import os # Set username and password
    os.environ['GRIDAPPSD_USER'] = 'system'
    os.environ['GRIDAPPSD_PASSWORD'] = 'manager'
    # Connect to GridAPPS-D Platform
    gapps = GridAPPSD(simulation_id)
    assert gapps.connected
    masters = []
    data_loc = '.'
    if 'Darwin' != platform.system():
        #data_loc = '/media/sf_git_scada_read/GridAppsD_Usecase3/701_disag/701/test'
        data_loc='/home/gridapps-d/pydnp3_old/gridappsd-dnp3-NREL_use_case_3/Use_case_6_Excel'
        #dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict_master.json", model_line_dict="model_line_dict.json")
        dnp3_to_cim = CIMMapping(conversion_dict=os.path.join(data_loc,"conversion_dict_master.json"), model_line_dict=os.path.join(data_loc,"model_line_dict_master.json"))
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
        if name == 'RTU1':
            global myCIMProcessor
            conversion_dict=os.path.join(data_loc,"conversion_dict_master.json")
            with open(conversion_dict) as f:
                conversion_dict = json.load(f)
           
            # pv_point_tmp1 = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=1, op_type=None)
            # pv_point_tmp1.measurement_id = "_5D0562C7-FE25-4FEE-851E-8ADCD69CED3B"
            # pv_point_tmp2 = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=2, op_type=None)
            # pv_point_tmp2.measurement_id = "_5D0562C7-FE25-4FEE-851E-8ADCD69CED3B"
            # pv_points = [pv_point_tmp1,pv_point_tmp2]
            pv_points = []
           # print('conversion_dict',conversion_dict['RTU1']['Analog output'].items())
            if 'RTU1' in conversion_dict and 'Analog output' in conversion_dict['RTU1']:
                
                for k, v in conversion_dict['RTU1']['Analog output'].items():
                    
                    if 'CIM mRID' in v:
                        pv_point = PointValue(command_type=None, function_code=None, value=10, point_def=0, index=int(v['index']), op_type=None)
                        pv_point.measurement_id = v['CIM mRID']
                        pv_point.attribute = v['CIM attribute']
                        pv_points.append(pv_point)
                #print('hi NREL DNP3 work',pv_points)

            # just build points for PV
            
            point_definitions = None

            myCIMProcessor = CIMProcessor(pv_points,application_1)

    comID=gapps.subscribe('/topic/goss.gridappsd.simulation.output.' + str(simulation_id), on_message)
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
    # application_1.slow_scan.Demand()

    # application_1.fast_scan_all.Demand()

    # for master in masters:
    #     master.fast_scan_all.Demand()
    msg_count=0
    csv_dict = {}
    #print('hi message ',message)
    cim_full_msg = {'simulation_id': simulation_id, 'timestamp': str(int(time.time())), 'irradiance':0.0, 'message':{'measurements':{}}}
    starttime = time.time()
    while True:
        current_time = time.time()
        # cim_full_msg = {'simulation_id': 1234, 'timestamp': 0, 'message':{'measurements':{}}}
        for master in masters:
            #print("getting CIM from master "+master.name)
            cim_msg = master.soe_handler.get_msg()
            #print('cim_msg',cim_msg)
            dnp3_msg_AI = master.soe_handler.get_dnp3_msg_AI()
           # print('dnp3_msg_AI',dnp3_msg_AI)
            dnp3_msg_BI = master.soe_handler.get_dnp3_msg_BI()
            if master.name =='RTU1':
                dnp3_msg_AO = myCIMProcessor.get_dnp3_msg_AO()
                dnp3_msg_BO = myCIMProcessor.get_dnp3_msg_BO()
            # print('keys',list(dnp3_msg.keys()))
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

            # print(cim_msg)
            #print('',message['message']['measurements'])
            cim_full_msg['message']['measurements'].update(cim_msg)
            cim_full_msg['timestamp'] = str(int(time.time()))
            
        with open('meas_map.json', 'w') as outfile:
            json.dump(cim_full_msg, outfile, indent=2)
        gapps.send('/topic/goss.gridappsd.simulation.input.'+str(simulation_id), json.dumps(cim_full_msg))
        msg_count+=1
        #print("message count "+ str(msg_count))
        #_log.info("message count "+ str(msg_count))

        #print(master.name+" " +str(cim_full_msg)[:100])
        #_log.info(cim_full_msg)
        # time.sleep(2)
        time.sleep(60.0 - ((time.time() - starttime) % 60.0))


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
    # parser.add_argument("feeder_info",help='feeder info directory for y-matrix, etc.')
    args = parser.parse_args()
    #print(args.names)
    names = args.names
    
    fid_select='_057AA112-989B-458C-8B34-31A29E94F220'    # Xcel feeder with all single phase DERs updated
    sim_id = Simulation_ID_GridLABD.start_test(fid_select, 86400)
    print('simulation ID created', sim_id)

    simulation_id = sim_id

    with open("device_ip_port_config_all_13bus.json") as f:
        device_ip_port_config_all_13bus = json.load(f)
    if args.names[0] == '*':
        names = []
        device_names = device_ip_port_config_all_13bus.keys()
        for device_name in device_names:
            if 'RTU' in device_name:
                names.append(device_name)
            if 'shark' in device_name[:5]:
                names.append(device_name)
            # if 'capbank' in device_name:
            #     names.append(device_name)

    #print("Running "+ str(names))
    time.sleep(1)

    device_ip_port_dict = device_ip_port_config_all_13bus[names[0]]
    print(device_ip_port_dict)
    run_master(device_ip_port_config_all_13bus, names,simulation_id)
