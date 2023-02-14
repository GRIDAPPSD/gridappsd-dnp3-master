# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#
# Copyright 2018, SLAC / 8minutenergy / Kisensum.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This material was prepared in part as an account of work sponsored by an agency of
# the United States Government. Neither the United States Government nor the
# United States Department of Energy, nor SLAC, nor 8minutenergy, nor Kisensum, nor any of their
# employees, nor any jurisdiction or organization that has cooperated in the
# development of these materials, makes any warranty, express or
# implied, or assumes any legal liability or responsibility for the accuracy,
# completeness, or usefulness or any information, apparatus, product,
# software, or process disclosed, or represents that its use would not infringe
# privately owned rights. Reference herein to any specific commercial product,
# process, or service by trade name, trademark, manufacturer, or otherwise
# does not necessarily constitute or imply its endorsement, recommendation, or
# favoring by the United States Government or any agency thereof, or
# SLAC, 8minutenergy, or Kisensum. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
# }}}

import logging
import numbers
from pickle import TRUE
import sys
import time
import yaml
import json
import numpy as np
import threading

from gridappsd.topics import simulation_output_topic, simulation_input_topic
from gridappsd import GridAPPSD, DifferenceBuilder, utils

from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
from dnp3.visitors import *

FILTERS = opendnp3.levels.NORMAL | opendnp3.levels.ALL_COMMS
FILTERS = opendnp3.levels.NOTHING
# HOST = "127.0.0.1"
# HOST = "192.168.1.2"
# LOCAL = "0.0.0.0"
# # LOCAL = "192.168.1.2"
# PORT = 20000


stdout_stream = logging.StreamHandler(sys.stdout)
stdout_stream.setFormatter(logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s'))

_log = logging.getLogger(__name__)
_log.addHandler(stdout_stream)
_log.setLevel(logging.DEBUG)


class MyMaster:
    """
        Interface for all master application callback info except for measurement values.

        DNP3 spec section 5.1.6.1:
            The Application Layer provides the following services for the DNP3 User Layer in a master:
                - Formats requests directed to one or more outstations.
                - Notifies the DNP3 User Layer when new data or information arrives from an outstation.

        DNP spec section 5.1.6.3:
            The Application Layer requires specific services from the layers beneath it.
                - Partitioning of fragments into smaller portions for transport reliability.
                - Knowledge of which device(s) were the source of received messages.
                - Transmission of messages to specific devices or to all devices.
                - Message integrity (i.e., error-free reception and transmission of messages).
                - Knowledge of the time when messages arrive.
                - Either precise times of transmission or the ability to set time values
                  into outgoing messages.
    """
    def __init__(self,
                 HOST="192.168.1.2", # "127.0.0.1
                 LOCAL= "0.0.0.0",
                 PORT=20000,
                 DNP3_ADDR=1,
                 LocalAddr=0,
                 log_handler=asiodnp3.ConsoleLogger().Create(),
                 listener=asiodnp3.PrintingChannelListener().Create(),
                 soe_handler=asiodnp3.PrintingSOEHandler().Create(),
                 master_application=asiodnp3.DefaultMasterApplication().Create(),
                 stack_config=None):

        _log.debug('Creating a DNP3Manager.')
        self.log_handler = log_handler
        self.manager = asiodnp3.DNP3Manager(1, self.log_handler)

        _log.debug('Creating the DNP3 channel, a TCP client.')
        self.retry = asiopal.ChannelRetry().Default()
        self.listener = listener
        self.channel = self.manager.AddTCPClient("tcpclient",
                                        FILTERS,
                                        self.retry,
                                        HOST,
                                        LOCAL,
                                        PORT,
                                        self.listener)

        _log.debug('Configuring the DNP3 stack.')
        self.stack_config = stack_config
        if not self.stack_config:
            self.stack_config = asiodnp3.MasterStackConfig()
            self.stack_config.master.responseTimeout = openpal.TimeDuration().Seconds(2)
            self.stack_config.link.RemoteAddr = 1024  ## TODO get from config Was 10
            self.stack_config.link.RemoteAddr = DNP3_ADDR
            self.stack_config.link.LocalAddr = LocalAddr
        print(self.stack_config.link.RemoteAddr, self.stack_config.link.LocalAddr)

        _log.debug('Adding the master to the channel.')
        self.soe_handler = soe_handler
        self.master_application = master_application
        self.master = self.channel.AddMaster("master",
                                   # asiodnp3.PrintingSOEHandler().Create(),
                                   self.soe_handler,
                                   self.master_application,
                                   self.stack_config)

        _log.debug('Configuring some scans (periodic reads).')
        # Set up a "slow scan", an infrequent integrity poll that requests events and static data for all classes.
        self.slow_scan = self.master.AddClassScan(opendnp3.ClassField().AllClasses(),
                                                  openpal.TimeDuration().Minutes(30),
                                                  opendnp3.TaskConfig().Default())
        # Set up a "fast scan", a relatively-frequent exception poll that requests events and class 1 static data.
        self.fast_scan = self.master.AddClassScan(opendnp3.ClassField(opendnp3.ClassField.CLASS_1),
                                                  openpal.TimeDuration().Minutes(1),
                                                  opendnp3.TaskConfig().Default())

        self.fast_scan_all = self.master.AddClassScan(opendnp3.ClassField().AllClasses(),
                                                  openpal.TimeDuration().Minutes(1),
                                                  opendnp3.TaskConfig().Default())

        # self.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        # self.master.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        self.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.NOTHING))
        self.master.SetLogFilters(openpal.LogFilters(opendnp3.levels.NOTHING))


        _log.debug('Enabling the master. At this point, traffic will start to flow between the Master and Outstations.')
        self.master.Enable()
        time.sleep(5)

    @classmethod
    def get_agent(cls):
        """Return the singleton DNP3Agent """
        agt = cls.agent
        if agt is None:
            raise ValueError('Master has no configured agent')
        return agt

    @classmethod
    def set_agent(cls, agent):
        """Set the singleton DNP3Agent """
        cls.agent = agent

    def send_direct_operate_command(self, command, index, callback=asiodnp3.PrintingCommandCallback.Get(),
                                    config=opendnp3.TaskConfig().Default()):
        """
            Direct operate a single command

        :param command: command to operate
        :param index: index of the command
        :param callback: callback that will be invoked upon completion or failure
        :param config: optional configuration that controls normal callbacks and allows the user to be specified for SA
        """
        self.master.DirectOperate(command, index, callback, config)

    def send_direct_operate_command_set(self, command_set, callback=asiodnp3.PrintingCommandCallback.Get(),
                                        config=opendnp3.TaskConfig().Default()):
        """
            Direct operate a set of commands

        :param command_set: set of command headers
        :param callback: callback that will be invoked upon completion or failure
        :param config: optional configuration that controls normal callbacks and allows the user to be specified for SA
        """
        self.master.DirectOperate(command_set, callback, config)

    def send_select_and_operate_command(self, command, index, callback=asiodnp3.PrintingCommandCallback.Get(),
                                        config=opendnp3.TaskConfig().Default()):
        """
            Select and operate a single command

        :param command: command to operate
        :param index: index of the command
        :param callback: callback that will be invoked upon completion or failure
        :param config: optional configuration that controls normal callbacks and allows the user to be specified for SA
        """
        self.master.SelectAndOperate(command, index, callback, config)

    def send_select_and_operate_command_set(self, command_set, callback=asiodnp3.PrintingCommandCallback.Get(),
                                            config=opendnp3.TaskConfig().Default()):
        """
            Select and operate a set of commands

        :param command_set: set of command headers
        :param callback: callback that will be invoked upon completion or failure
        :param config: optional configuration that controls normal callbacks and allows the user to be specified for SA
        """
        self.master.SelectAndOperate(command_set, callback, config)

    def shutdown(self):
        del self.slow_scan
        del self.fast_scan
        del self.master
        del self.channel
        self.manager.Shutdown()


class MyLogger(openpal.ILogHandler):
    """
        Override ILogHandler in this manner to implement application-specific logging behavior.
    """

    def __init__(self):
        super(MyLogger, self).__init__()

    def Log(self, entry):
        flag = opendnp3.LogFlagToString(entry.filters.GetBitfield())
        filters = entry.filters.GetBitfield()
        location = entry.location.rsplit('/')[-1] if entry.location else ''
        message = entry.message
        _log.debug('LOG\t\t{:<10}\tfilters={:<5}\tlocation={:<25}\tentry={}'.format(flag, filters, location, message))
        # pass


class AppChannelListener(asiodnp3.IChannelListener):
    """
        Override IChannelListener in this manner to implement application-specific channel behavior.
    """

    def __init__(self):
        super(AppChannelListener, self).__init__()

    def OnStateChange(self, state):
        # _log.debug('In AppChannelListener.OnStateChange: state={}'.format(opendnp3.ChannelStateToString(state)))
        pass




def on_message(self, message):
    json_msg = yaml.safe_load(str(message))
    #print(json_msg)

class SOEHandlerSimple(opendnp3.ISOEHandler):
    """
        Override ISOEHandler in this manner to implement application-specific sequence-of-events behavior.

        This is an interface for SequenceOfEvents (SOE) callbacks from the Master stack to the application layer.
    """

    def __init__(self):
        super(SOEHandlerSimple, self).__init__()

    def Process(self, info, values):
        """
            Process measurement data.

        :param info: HeaderInfo
        :param values: A collection of values received from the Outstation (various data types are possible).
        """
        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalog,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval
        }
        visitor_class = visitor_class_types[type(values)]
        visitor = visitor_class()
        values.Foreach(visitor)
        for index, value in visitor.index_and_value:
            log_string = 'SOEHandler.Process {0}\theaderIndex={1}\tdata_type={2}\tindex={3}\tvalue={4}'
           # log_string = 'SOEHandler.Process {0}\theaderIndex={1}\tvalue={4}'
            _log.debug(log_string.format(info.gv, info.headerIndex, type(values).__name__, index, value))

    def Start(self):
        _log.debug('In SOEHandler.Start')

    def End(self):
        _log.debug('In SOEHandler.End')


class SOEHandler(opendnp3.ISOEHandler):
    """
        Override ISOEHandler in this manner to implement application-specific sequence-of-events behavior.

        This is an interface for SequenceOfEvents (SOE) callbacks from the Master stack to the application layer.
    """

    def __init__(self, name, device, dnp3_to_cim,gapps):
        self._name = name
        self._device = device
        self._dnp3_to_cim = dnp3_to_cim
        self.CIM_msg = {}
        self.Get_CIM_Msg={}
        self._dnp3_msg_AI = {}
        self._dnp3_msg_AI_header = []
        self._dnp3_msg_BI = {}
        self._dnp3_msg_BI_header = []
        self.lock = threading.Lock()
        self.gapps=gapps

        super(SOEHandler, self).__init__()

    def get_msg(self):
        with self.lock:
            # time.sleep(2)
            return self.Get_CIM_Msg

    def get_dnp3_msg_AI(self):
        with self.lock:
            print('self._dnp3_msg_AI',len(self._dnp3_msg_AI))
            return self._dnp3_msg_AI

    def get_dnp3_msg_AI_header(self):
        with self.lock:
            return self._dnp3_msg_AI_header

    def get_dnp3_msg_BI(self):
        with self.lock:
            return self._dnp3_msg_BI

    def get_dnp3_msg_BI_header(self):
        with self.lock:
            return self._dnp3_msg_BI_header

    def update_cim_msg_analog_multi_index(self, CIM_msg, index, value, conversion, model):
        # model_line_dict['irradiance']
        if conversion[index]['CIM name'] == 'irradiance':
            CIM_msg['irradiance'] = value
            print('irradiance', value)
            return
        CIM_phase = conversion[index]['CIM phase']
        CIM_units = conversion[index]['CIM units']
        CIM_type = conversion[index]['CIM type']
        
        CIM_attribute = conversion[index]['CIM attribute']
        ## Check if multiplier is na or str
        multiplier = conversion[index]['Multiplier']
        if CIM_type not in model:
            print(str(CIM_units) + ' not in model')
            return

        if CIM_phase not in model[CIM_type]:
            print(str(model) + ' phase not correct in model', CIM_phase, CIM_type)
            return
        mrid = model[CIM_type][CIM_phase]['mrid']
        if type(multiplier) == str:
            multiplier = 1

        CIM_value = {'mrid': mrid, 'angle': 0}
        if mrid not in CIM_msg:
            CIM_msg[mrid] = CIM_value
        CIM_msg[mrid][CIM_attribute] = value * multiplier  # times multipier


    def update_cim_msg_analog(self, CIM_msg, index, value, conversion, model):
        if 'Analog input' in conversion and index in conversion['Analog input']:
            CIM_phase = conversion['Analog input'][index]['CIM phase']
            CIM_units = conversion['Analog input'][index]['CIM units']
            CIM_attribute = conversion['Analog input'][index]['CIM attribute']
            ## Check if multiplier is na or str
            multiplier = conversion['Analog input'][index]['Multiplier']
            if CIM_units not in model:
                print(str(CIM_units) +' not in model')
                return

            if CIM_phase not in model[CIM_units]:
                print(str(CIM_units) +' phase not correct in model')
                return
                
            mrid = model[CIM_units][CIM_phase]['mrid']
            if type(multiplier) == str:
                multiplier = 1

            CIM_value = {'mrid': mrid}
            if CIM_units == 'PNV' or CIM_units == 'VA':
                CIM_value = {'mrid': mrid, 'magnitude': 0, 'angle': 0}

            if mrid not in CIM_msg:
                CIM_msg[mrid] = CIM_value
            CIM_msg[mrid][CIM_attribute] = value * multiplier  # times multiplier

    def update_cim_msg_binary_rtu(self, CIM_msg, index, value, conversion,model):
        #     print(conversion['Binary input'][index])
        print('binary jeff', conversion[index]['CIM phase'])

        CIM_phases = conversion[index]['CIM phase']
        CIM_units = conversion[index]['CIM units']
        CIM_attribute = conversion[index]['CIM attribute']
        print('binary phases',CIM_phases, CIM_units) 
        print(type(CIM_units),CIM_units)
        if CIM_units not in model:
            print(str(CIM_units) +' not in model')
            return
        for CIM_phase in CIM_phases:
            # print(model)
            # exit(0)
            mrid = model[CIM_units][CIM_phase]['mrid']
            CIM_value = {'mrid': mrid}
            if mrid not in CIM_msg:
                CIM_msg[mrid] = CIM_value
            int_value = 1
            if value:
                int_value=0
            CIM_msg[mrid][CIM_attribute] = int_value
            print('binary',value, int_value)

    def update_cim_msg_binary(self, CIM_msg, index, value, conversion,model):
        #     print(conversion['Binary input'][index])
        print('binary jeff', model)
        if 'Binary input' in conversion and index in conversion['Binary input']:
            CIM_phases = conversion['Binary input'][index]['CIM phase']
            CIM_units = conversion['Binary input'][index]['CIM units']
            CIM_attribute = conversion['Binary input'][index]['CIM attribute']
            print('binary phases',CIM_phases)
            print(type(CIM_units),CIM_units)
            if CIM_units not in model:
                print(str(CIM_units) +' not in model')
                return
            for CIM_phase in CIM_phases:
                # print(model)
                # exit(0)
                mrid = model[CIM_units][CIM_phase]['mrid']
                CIM_value = {'mrid': mrid}
                if mrid not in CIM_msg:
                    CIM_msg[mrid] = CIM_value
                CIM_msg[mrid][CIM_attribute] = value 
                print('binary',value)


    def Process(self, info, values):
        """
            Process measurement data.

        :param info: HeaderInfo
        :param values: A collection of values received from the Outstation (various data types are possible).
        """
        visitor_class_types = {
            opendnp3.ICollectionIndexedBinary: VisitorIndexedBinary,
            opendnp3.ICollectionIndexedDoubleBitBinary: VisitorIndexedDoubleBitBinary,
            opendnp3.ICollectionIndexedCounter: VisitorIndexedCounter,
            opendnp3.ICollectionIndexedFrozenCounter: VisitorIndexedFrozenCounter,
            opendnp3.ICollectionIndexedAnalog: VisitorIndexedAnalog,
            opendnp3.ICollectionIndexedBinaryOutputStatus: VisitorIndexedBinaryOutputStatus,
            opendnp3.ICollectionIndexedAnalogOutputStatus: VisitorIndexedAnalogOutputStatus,
            opendnp3.ICollectionIndexedTimeAndInterval: VisitorIndexedTimeAndInterval
        }
        visitor_class = visitor_class_types[type(values)]
        visitor = visitor_class()
        values.Foreach(visitor)

        conversion_dict = self._dnp3_to_cim.conversion_dict
        model_line_dict = self._dnp3_to_cim.model_line_dict

        model = model_line_dict[self._name]
        conversion = conversion_dict[self._device]
        
        #print('devices',self._device)
        if self._device == 'Shark':
            conversion_name_index_dict = {v['Index']: v for k, v in conversion['Analog input'].items()}
        else:
            conversion_name_index_dict = {v['index']: v for k, v in conversion['Analog input'].items()}
    
        
        #-------------------------------------------
        with self.lock:
            #print('hi how are you',visitor.index_and_value)
            if type(values) == opendnp3.ICollectionIndexedAnalog:
                
                for index, value in visitor.index_and_value:

                    if not self._dnp3_msg_AI_header:
                        # self._dnp3_msg_AI_header = {v['index']: v['CIM name'] for k, v in conversion['Analog input'].items()}
                        if self._device == 'Shark': 
                            self._dnp3_msg_AI_header = [v['Type'] for k, v in conversion['Analog input'].items()]
                        else:
                            self._dnp3_msg_AI_header = [v['CIM name']+'_'+v['CIM units'] for k, v in conversion['Analog input'].items()]
                    
                    ######################################################################################################################
                    if 'RTU' in self._device:
                        not_found = True
                        self._dnp3_msg_AI[index]=value
                       
                        for coin in list(conversion_name_index_dict.keys()):
                            
                            if index == conversion_name_index_dict[coin]['index']:
                                model = model_line_dict[conversion_name_index_dict[index]['CIM name']]
                                CIM_phase = conversion_name_index_dict[index]['CIM phase']
                                CIM_type = conversion_name_index_dict[index]['CIM type']
                                CIM_Variable = conversion_name_index_dict[index]['CIM Variable']
                                mrid = model[CIM_type][CIM_phase]['mrid']
                                
                                if index != 0:
                                    if CIM_Variable =='P':
                                        magnitude = self._dnp3_msg_AI[index]
                                        self.Get_CIM_Msg[mrid]={'mrid':mrid,'magnitude':magnitude,'angle':0} 
                                    elif  CIM_Variable =='V':
                                        magnitude = self._dnp3_msg_AI[index]                                         
                                        self.Get_CIM_Msg[mrid]={'mrid':mrid,'magnitude':magnitude,'angle':0}
                                else:
                                    if CIM_Variable =='P':
                                        magnitude = self._dnp3_msg_AI[index] 
                                        self.Get_CIM_Msg[mrid]={'mrid':mrid,'magnitude':magnitude,'angle':0}
                                        #print('hi dnp3 AI',self.Get_CIM_Msg)
                                not_found = False
                        if not_found:
                            print('AI',value)
                            _log.debug("No conversion for " + str(index))
                    ######################################################################################################################
                    elif isinstance(value, numbers.Number) and str(float(index)) in conversion['Analog input']:
                        self._dnp3_msg_AI[index]=value
                        self.update_cim_msg_analog(self.CIM_msg, str(float(index)), value, conversion, model)
                    elif str(index) in conversion['Analog input']:
                        #print('I am from USA')
                        self._dnp3_msg_AI[index]=value
                        self.update_cim_msg_analog(self.CIM_msg, str(index), value, conversion, model)
                    else:
                        print(" No entry for index " + str(index))
            elif type(values) == opendnp3.ICollectionIndexedBinary:
                if 'RTU' in self._device and 'Binary input' in conversion:
                    
                    for index, value in visitor.index_and_value:
                        self._dnp3_msg_BI[index]=value
                        conversion_name_index_dict = {v['index']: v for k, v in conversion['Binary input'].items()}
                        #for counter2 in list(conversion_name_index_dict.keys()):
                        #    print('Hi check BI',index,counter2)
                        if index == conversion_name_index_dict[index]['index']:  
                                model = model_line_dict[conversion_name_index_dict[index]['CIM name']]
                                CIM_phase = conversion_name_index_dict[index]['CIM phase']
                                CIM_type = conversion_name_index_dict[index]['CIM type']
                                CIM_Variable = conversion_name_index_dict[index]['CIM Variable']
                                mrid = model[CIM_type][CIM_phase]['mrid'] 
                                self.Get_CIM_Msg[mrid]={'mrid':mrid,'magnitude':value,'angle':0} 
                                
                        else:
                                _log.debug("No conversion for " + str(index))
                                #print('AO',value,counter2)
                else:
                    print('Colarado NREL')
                    for index, value in visitor.index_and_value:
                        self.update_cim_msg_binary(self.CIM_msg, str(float(index)), value, conversion, model) # Untested might work
            else:
                for index, value in visitor.index_and_value:
                    pass

    def Start(self):
        _log.debug('In SOEHandler.Start')

    def End(self):
        _log.debug('In SOEHandler.End')


class MasterApplication(opendnp3.IMasterApplication):
    def __init__(self):
        super(MasterApplication, self).__init__()

    # Overridden method
    def AssignClassDuringStartup(self):
        _log.debug('In MasterApplication.AssignClassDuringStartup')
        return False

    # Overridden method
    def OnClose(self):
        _log.debug('In MasterApplication.OnClose')

    # Overridden method
    def OnOpen(self):
        _log.debug('In MasterApplication.OnOpen')

    # Overridden method
    def OnReceiveIIN(self, iin):
        _log.debug('In MasterApplication.OnReceiveIIN')

    # Overridden method
    def OnTaskComplete(self, info):
        _log.debug('In MasterApplication.OnTaskComplete')

    # Overridden method
    def OnTaskStart(self, type, id):
        _log.debug('In MasterApplication.OnTaskStart')


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
    print(result.__doc__)


def command_callback(result=None):
    """
    :type result: opendnp3.ICommandTaskResult
    """
    print("Received command result with summary: {}".format(opendnp3.TaskCompletionToString(result.summary)))
    result.ForeachItem(collection_callback)


def restart_callback(result=opendnp3.RestartOperationResult()):
    if result.summary == opendnp3.TaskCompletion.SUCCESS:
        print("Restart success | Restart Time: {}".format(result.restartTime.GetMilliseconds()))
    else:
        print("Restart fail | Failure: {}".format(opendnp3.TaskCompletionToString(result.summary)))


def main():
    """The Master has been started from the command line. Execute ad-hoc tests if desired."""
    # app = MyMaster()
    app = MyMaster(log_handler=MyLogger(),
                   listener=AppChannelListener(),
                   soe_handler=SOEHandler(),
                   master_application=MasterApplication())
    _log.debug('Initialization complete. In command loop.')
    # Ad-hoc tests can be performed at this point. See master_cmd.py for examples.
    app.shutdown()
    _log.debug('Exiting.')
    exit()


if __name__ == '__main__':
    main()
