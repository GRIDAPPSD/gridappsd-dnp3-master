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
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass

import gridappsd.topics as topics
import numpy as np
import yaml
from dnp3.CIMPro_AIAO_BIBO import CIMProcessor

# from dnp3.master_pnnl import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication
from dnp3.dnp3_to_cim import CIMMapping
from dnp3.points import PointValue

# from dnp3_python.dnp3station.master import MyMaster
from dnp3_python.dnp3station.master_new import MyMasterNew
from gridappsd import DifferenceBuilder, GridAPPSD, utils
from gridappsd.topics import (
    field_input_topic,field_output_topic,
    simulation_input_topic,
    simulation_output_topic,
)
from pydnp3 import opendnp3, openpal

# Setup argparse
parser = argparse.ArgumentParser()
parser.add_argument(
    "outstation_names", nargs="+", help="name of dnp3 outstation", type=str
)
parser.add_argument(
    "--config_path",
    help="Path of the folder containing input configuration files",
    default="config",
    type=str,
)

# Parse arguments globally
args = parser.parse_args()

# Now args is available globally within the script
outstation_names = args.outstation_names
config_path = args.config_path


@dataclass
class RTUConfig:
    """
    A configuration class for RTU (Remote Terminal Unit) devices.

    This class holds all the necessary configuration details for an RTU,
    allowing easy access and management of its properties. It also includes
    a static method to load configurations from a JSON file, simplifying
    initialization and reducing potential setup errors.

    Attributes:
        name (str): The name of the RTU.
        conversion_type (str): Type of conversion used by the RTU.
        CIM_object (str): CIM object identifier.
        port (int): Network port number the RTU uses for communication.
        ip (str): IP address of the RTU.
        desc (str): A brief description of the RTU.
        link_local_addr (int): Local network address used for linking.
        link_remote_addr (int): Remote network address used for linking.
    """

    name: str
    conversion_type: str
    CIM_object: str
    port: int
    ip: str
    desc: str
    link_local_addr: int
    link_remote_addr: int

    @staticmethod
    def load_json_config(file_path: str, rtu_key: str) -> RTUConfig:
        """
        Load RTU configuration from a JSON file.

        :param file_path: Path to the JSON file containing the RTU configurations.
        :param rtu_key: Key of the RTU entry to be loaded from the JSON file.
        :return: An instance of RTUConfig initialized with data from the JSON file.

        # Example usage: Assuming the JSON file is located at 'config.json'
        # and the desired RTU key is 'RTU1'
        rtu_config = RTUConfig.load_json_config('config.json', 'RTU1')
        if rtu_config:
            print(rtu_config)

        Example config file:
        {
            "RTU1": {
                "name": "RTU1",
                "conversion_type": "RTU1",
                "CIM object": "l50",
                "port": "30000",
                "ip": "127.0.0.1",
                "desc": "RTU1",
                "link_local_addr": "2",
                "link_remote_addr": "1"
            },
            "RTU2": {
                "name": "RTU2",
                "conversion_type": "RTU2",
                "CIM object": "l50",
                "port": "40000",
                "ip": "127.0.0.1",
                "desc": "RTU2",
                "link_local_addr": "2",
                "link_remote_addr": "1"
            }
        }
        """
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                rtu_data = data[rtu_key]
                return RTUConfig(
                    name=rtu_data["name"],
                    conversion_type=rtu_data["conversion_type"],
                    CIM_object=rtu_data["CIM object"],
                    port=int(rtu_data["port"]),
                    ip=rtu_data["ip"],
                    desc=rtu_data["desc"],
                    link_local_addr=int(rtu_data["link_local_addr"]),
                    link_remote_addr=int(rtu_data["link_remote_addr"]),
                )

        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            raise Exception(f"Error loading RTU configuration: {e}")


myCIMProcessor = None

logging.basicConfig(level=logging.DEBUG)
_log = logging.getLogger(__name__)


def on_message_control_outstation_binaryOutput(headers, message):
    """
    A callback function when the receiving message from a subscribed topic.
    message: a cim-difference message
    """
    _log.debug(f"{headers = }")
    _log.debug(f"{message = }")
    timestamp = message["input"]["message"]["timestamp"]  # 1357048800
    forward_differences = message[
        "input"
    ][
        "message"
    ][
        "forward_differences"
    ]  # [{'object': '61A547FB-9F68-5635-BB4C-F7F537FD824E', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': 'E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA', 'attribute': 'ShuntCompensator.sections', 'value': 1}]
    _log.debug(f"{timestamp = }")
    _log.debug(f"{forward_differences = }")

    # register_to_db_index: dict[str, int] = {
    #     "61A547FB-9F68-5635-BB4C-F7F537FD824E": 0,
    #     "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA": 1,
    # }  # TODO: confirm what should be the correct mrid(s).
    with open(os.path.join(config_path, "mrid_object_outstation_index_dict_receive.json")) as f:
        register_to_db_index_receive = json.load(f)
    rtu = outstation_names[
        0
    ]  # TODO: assume only one RTU device for now (not sure if will demo multiple devcies)
    for command in forward_differences:
        master_app: MyMasterNew = master_apps[rtu]
        mrid = command["object"]  # aka object in the cim-difference-message
        index = register_to_db_index_receive[mrid]
        val_to_set = (
            True if command["value"] == True else False
        )  # TODO: Make sure if True (Enable) -> True, False (Disable) -> False. It is possible gridapps-d has different interpretation of True/Enable vs. dnp3
        master_app.send_direct_point_command(
            group=10,
            variation=2,
            index=index,
            val_to_set=val_to_set,
        )  # Note: group10Variation2 is for BinaryOutput, hardcoded here for demo purposes
        # result = master_application.get_db_by_group_variation(group=10, variation=2)
        # print("SUCCESS", {"BinaryOutputStatus": list(result.values())[0]})


# def _register_mapping(register_name: str, db_data):
#     """# mapping based on register name, e.g., ufls_59.1" -> "BinaryOutputStatus"[0]"""
#     if register_name == "ufls_59.1":
#         return db_data["BinaryOutputStatus"][0]
#     else:
#         return db_data["BinaryOutputStatus"][1]


def _construct_cim_full_msg(
    db_data,
    cim_full_msg=None,
) -> dict:
    """
    Constructs a CIM message with measurement data.
    Args:
        cim_full_msg (dict, optional): Initial structure of the CIM message. Defaults to None.
    Returns:
        dict: A dictionary containing the CIM message structured with measurements.
    Example message:
    {
        "simulation_id" : "12ae2345",
        "message" : {
            "timestamp" : "1357048800",
            "measurements" : {
                "123a456b-789c-012d-345e-678f901a234b":{
                                    "measurement_mrid" : "123a456b-789c-012d-345e-678f901a234b",
                                    "value": True  # enable is True, disable is False
                                    # "magnitude" : 3410.456,
                                    # "angle" : -123.456
                                    }
                            }
                    }
    }
    """
    if cim_full_msg is None:
        cim_full_msg = {"message": {}}

    # Load measurement definitions from a JSON file.
    # config_path = "path_to_config"  # Define or import the configuration path.
    with open(os.path.join(config_path, "new_measurement_dict_master.json")) as f:
        new_measurement_dict_master = json.load(f)
        
    with open(os.path.join(config_path, "mrid_object_outstation_index_dict_send.json")) as f:
        register_to_db_index_send = json.load(f)

    # Populate the CIM message with the latest measurements.
    for key, value in new_measurement_dict_master.items():
        mr_id = value["Pos"]["ABC"][0]["mrid"]
        # Assuming _register_mapping and db_data are defined elsewhere
        value = (
            True if db_data["BinaryOutputStatus"][register_to_db_index_send[mrid]] else False
        )  # Note: db_data can be None
        current_time = int(time.time())
        cim_full_msg["message"]["timestamp"] = str(current_time)
        cim_full_msg["message"]["measurements"] = {
            mr_id: {"measurement_mrid": mr_id, "value": value}
        }
        # Log the prepared message
        _log.debug(f"Publishing CIM measurement XXXX: {json.dumps(cim_full_msg)}")

    return cim_full_msg


def run_master():
    # init master station(s)
    config_path = args.config_path
    _log.debug(f"{config_path = }")
    # Note: in current design master_apps is a collection of master applications.
    # Make it global accessible to use in gridapps-d on_message callback workflow
    global master_apps
    master_apps = {}
    for outstaion_name in outstation_names:
        config: RTUConfig = RTUConfig.load_json_config(
            os.path.join(config_path, "device_ip_port_config.json"), outstaion_name
        )
        master_app = MyMasterNew(
            master_ip="0.0.0.0",
            outstation_ip=config.ip,
            port=config.port,
            master_id=config.link_local_addr,
            outstation_id=config.link_remote_addr,
        )
        master_app.start()
        master_apps[outstaion_name] = master_app

    # subscribe to gridapps-d topic to receive control command
    topic = field_input_topic()  # topic = '/topic/goss.gridappsd.field.input'
    gapps.subscribe(
        topic, on_message_control_outstation_binaryOutput
    ) 
    # loop to poll data from outstation, construct a cim-message and send it
    msg_count = 0
    while True:
        for outstation_name, master_app in master_apps.items():
            # Send scan request and process data
            # time.sleep(100)  # TODO: clean this. 
            master_app.send_scan_all_request()
            db_data = master_app.soe_handler.db
            _log.debug(f"{db_data =}")

            try:
                cim_full_msg = _construct_cim_full_msg(db_data)
            except Exception as e:
                _log.error(f"Failed to construct CIM message: {e}")
                continue  # Continue with the next iteration if an error occurs.

            # Sending the cim-message
            # _log.info(f"Sending CIM message: {cim_full_msg}")
            gapps.send(
                topics.field_output_topic(), json.dumps(cim_full_msg)
            )
        msg_count += 1
        time.sleep(
            5
        )  # Wait for 2 seconds before the next cycle (to poll data from outstation)


if __name__ == "__main__":
    # simulation_id = "field_data"  # TODO: Confirm what simulation_id is for. Change dummy simulation id to field id (if it is needed)
    # gapps = GridAPPSD(stomp_address="10.15.223.157", stomp_port=61613)
    gapps = GridAPPSD()
    gapps.connect()

    # with open(config_path + "/device_ip_port_config.json") as f:
    #     device_ip_port_config_all_Xcel = json.load(f)

    # dnp3_to_cim = CIMMapping(
    #     conversion_dict=os.path.join(config_path, "conversion_dict_master_data.json"),
    #     model_line_dict=os.path.join(config_path, "measurement_dict_master.json"),
    # )

    # conversion_dict = dnp3_to_cim.conversion_dict
    # # print(conversion_dict)

    time.sleep(1)

    run_master()
