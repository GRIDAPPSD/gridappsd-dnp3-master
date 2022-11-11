import os
import sys
import time
sys.path.append("../dnp3/service")

from aster import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication
from dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal




def collection_callback(result=None):
    """
    :type result: opendnp3.CommandPointResult
    """
    print("Header: {0} | Index:  {1} | State:  {2} | Status: {3}".format(
        result.headerIndex,
        result.index,
        opendnp3.CommandPointStateToString(result.state),
        opendnp3.CommandStatusToString(result.status)
    ))
    # print(result)


def command_callback(result=None):
    """
    :type result: opendnp3.ICommandTaskResult
    """
    print("Received command result with summary: {}".format(opendnp3.TaskCompletionToString(result.summary)))
    result.ForeachItem(collection_callback)



# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_master(device_ip_port_config_all, names):
    masters = []
    dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict_master.json", model_line_dict="model_line_dict.json")
    for name in names:
        device_ip_port_dict = device_ip_port_config_all[name]
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
                                soe_handler=SOEHandler(object_name, convertion_type, dnp3_to_cim),
                                master_application=MasterApplication())
        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        # print('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))
        masters.append(application_1)

    SLEEP_SECONDS = 1
    time.sleep(SLEEP_SECONDS)
    #group_variation = opendnp3.GroupVariationID(32, 2)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 1')
    #application_1.master.ScanRange(group_variation, 0, 12)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 2')
    #application_1.master.ScanRange(opendnp3.GroupVariationID(32, 2), 0, 3, opendnp3.TaskConfig().Default())
    #time.sleep(SLEEP_SECONDS)
    print('\nReading status 3')
    # application_1.slow_scan.Demand()

    # application_1.fast_scan_all.Demand()

    # for master in masters:
    #     master.fast_scan_all.Demand()
    master = masters[0]

    # master.send_direct_operate_command_set(opendnp3.CommandSet(
    #         [
    #             opendnp3.WithIndex(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON), 0),
    #             opendnp3.WithIndex(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF), 1)
    #         ]),
    #         command_callback)

    test_cmd = True
    if test_cmd:
        open_cmd = False
        if open_cmd:
            # Open
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                                0, # PULSE/LATCH_ON to index 0 for open
                                                                command_callback)
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                                1, # PULSE/LATCH_ON to index 1 for open
                                                                command_callback)
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                                2, # PULSE/LATCH_ON to index 2 for open
                                                                command_callback)
        else:
            # Will need 5 minutes after open operation for this capbank 
            #Close
            # master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
            #                                                     0, # PULSE/LATCH_ON to index 0 for close
            #                                                     command_callback)
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                                0, # PULSE/LATCH_ON to index 0 for close
                                                                command_callback)
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                                1, # PULSE/LATCH_ON to index 0 for close
                                                                command_callback)                                                        
            master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                                2, # PULSE/LATCH_ON to index 0 for close
                                                                command_callback)
    # master.send_direct_operate_command(opendnp3.AnalogOutputInt32(7),
    #                                                  1,
    #                                                  command_callback)
    
    # master.send_direct_operate_command(opendnp3.AnalogOutputInt32(14),
    #                                                  2,
    #                                                  command_callback)



    while True:
        cim_full_msg = {'simulation_id': 1234, 'timestamp': 0, 'messages':{}}
        for master in masters:
            cim_msg = master.soe_handler.get_msg()
            # print(cim_msg)
            cim_full_msg['messages'].update(cim_msg)

        print(cim_full_msg)
        time.sleep(5)
        # master.send_select_and_operate_command()        
        # master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
        #                                                  1,
        #                                                  command_callback)


    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                        1,
                                                        command_callback)

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
    print(args.names)
    # exit(0)
    with open("device_ip_port_config_all.json") as f:
        device_ip_port_config_all = json.load(f)

    device_ip_port_dict = device_ip_port_config_all[args.names[0]]
    print(device_ip_port_dict)
    run_master(device_ip_port_config_all, args.names)
    # run_master(device_ip_port_dict['ip'],
    #            device_ip_port_dict['port'],
    #            device_ip_port_dict['link_local_addr'],
    #            device_ip_port_dict['conversion_type'],
    #            '632633')