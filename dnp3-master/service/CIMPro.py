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

#########################Edited/Prepared this code by Dr.Venkateswara Reddy Motakatla, NREL
from master import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication, SOEHandlerSimple
from dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
import yaml
from points import PointValue
import threading
import time
import logging
_log = logging.getLogger(__name__)


def collection_callback(result=None):
    """
    :type result: opendnp3.CommandPointResult
    """
    print("Header: {0} | Index:  {1} | State:  {2} | Status: {3}".format(
        result.headerIndex,
        result.index,
        opendnp3.CommandPointStateToString(result.state),
        opendnp3.CommandStatusToString(result.status)))


def command_callback(result=None):
    """
    :type result: opendnp3.ICommandTaskResult
    """
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

        cap_point1 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=0, op_type=None)
        cap_point1.measurement_id = "_61aeecc2-8594-4f01-ab35-9b9e9798475e"
        cap_point2 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=1, op_type=None)
        cap_point2.measurement_id = "_C1706031-2C1C-464C-8376-6A51FA70B470"
        cap_point3 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=2, op_type=None)
        cap_point3.measurement_id = "_245E3924-8292-46D5-A11E-C80F7D6EE253"
        self._cap_list = [cap_point1,cap_point2,cap_point3]

        for point in self._cap_list:
            self._dnp3_msg_BO[point.index] = point.value

        for point in self._pv_points:
            self._dnp3_msg_AO[point.measurement_id] = point.value

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
                    self._dnp3_msg_BO = {}
                    for point in self._cap_list:
                        time.sleep(.02)
                        # Capbank
                        if command.get("object") == point.measurement_id :
                            self._dnp3_msg_BO[point.index] = point.value
                        if command.get("object") == point.measurement_id and point.value != command.get("value"):
                            open_cmd = command.get("value") != 0
                            if open_cmd:
                                # Open
                                master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                                        point.index,  # PULSE/LATCH_ON to index 0 for open
                                                                        command_callback)
                                self._dnp3_msg_BO[point.index] = point.value
                                point.value = 0
                            else:
                                master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                                        point.index,  # PULSE/LATCH_ON to index 0 for open
                                                                        command_callback)
                                self._dnp3_msg_BO[point.index] = point.value
                                
                                point.value = 1
                        elif command.get("object") == point.measurement_id and point.value == command.get("value"):
                            print("Cap check", command.get("object"), command.get("value"))

                    # PV points
                    for point in self._pv_points:
                        if command.get("object") == point.measurement_id : # and point.value != command.get("value"):
                            if command.get("attribute") == point.attribute:
                                temp_index = point.index
                                point.value = int(command.get("value"))
                                self._dnp3_msg_AO[temp_index] = point.value
                                _log.info("PV ",point.index, point.value, point.attribute)
                                master.send_direct_operate_command(opendnp3.AnalogOutputInt32(point.value), temp_index, command_callback)