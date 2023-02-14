
from cmath import nan
from itertools import count
from dnp3.master_pnnl import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication, SOEHandlerSimple
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
import yaml
from dnp3.points import PointValue
import threading
import time
import logging
import numpy
_log = logging.getLogger(__name__)
from datetime import datetime, timezone

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
    print("Time summary: {}".format((datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))))
    print("Received command result with summary: {}".format(opendnp3.TaskCompletionToString(result.summary)))
    result.ForeachItem(collection_callback)

class CIMProcessor(object):

    def __init__(self, point_definitions,master):
        self.point_definitions = point_definitions # TODO
        self._master = master
        self._pv_points = point_definitions
        self._dnp3_msg_AO = {}
        self._dnp3_msg_BO = {}
        self._dnp3_msg_AO_header = []
        self._dnp3_msg_BO_header = []
        self.counter=0

        for point in self._pv_points:
            if point.attribute=='Switch.open':
                self._dnp3_msg_BO[point.index] = point.value
            else:
                self._dnp3_msg_AO[point.index] = point.value

        self.lock = threading.Lock()

    
    def get_dnp3_msg_AO(self):
        with self.lock:
            return self._dnp3_msg_AO

    def get_dnp3_msg_AO_header(self):
        with self.lock:
            return self._dnp3_msg_AO_header

    def get_dnp3_msg_BO(self):
        with self.lock:
            return self._dnp3_msg_BO

    def process(self, message):
        master = self._master
        json_msg = yaml.safe_load(str(message))
        
        if type(json_msg) != dict:
            raise ValueError(
                ' is not a json formatted string.'
                + '\njson_msg = {0}'.format(json_msg))

        if "input" in json_msg.keys():
            control_values = json_msg["input"]["message"]["forward_differences"]
            _log.info("control_values ", control_values)
            with self.lock:
                for command in control_values:
                    #------------------------- pv point definiations
                    for point in self._pv_points:
                        
                        if command.get("object") == point.measurement_id : # and point.value != command.get("value"):

                            if command.get("attribute") == point.attribute:
                                _log.info("PV ",point.index, point.value, point.attribute)
                                if point.attribute=='Switch.open':
                                    #-------------- Binary output part
                                    if point.value ==1:
                                        master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                                        point.index,  # PULSE/LATCH_ON to index 0 for open
                                                                        command_callback)
                                        print('Switch ON',point.index,point.value)
                                        point.value = 1
                                    else: 
                                        master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                                        point.index,  # PULSE/LATCH_ON to index 0 for open
                                                                        command_callback)
                                        print('Switch OFF',point.index,point.value)
                                        point.value = 0
                                else:
                                    #-------------- analog output part
                                    master.send_direct_operate_command(opendnp3.AnalogOutputInt32(point.value),
                                                                            point.index,
                                                                            command_callback)
