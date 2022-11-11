import sys
sys.path.append(".")

import json
import yaml
import sys
import datetime
import random
import uuid
import math
#import pydevd;pydevd.settrace(suspend=False) # Uncomment For Debugging on other Threads
from .oestester import OesTester

from collections import defaultdict
from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
from typing import List, Dict, Union, Any
from outstation import DNP3Outstation
from points import (
    PointArray, PointDefinitions, PointDefinition, DNP3Exception, POINT_TYPE_ANALOG_INPUT, POINT_TYPE_BINARY_INPUT
)

from master import command_callback, restart_callback

out_json = list()

'''Dictionary for mapping the attribute values of control poitns for Capacitor, Regulator and Switches'''

attribute_map = {
    "capacitors": {
        "attribute": ["RegulatingControl.enabled", "RegulatingControl.mode", "RegulatingControl.targetDeadband", "RegulatingControl.targetValue",
                      "ShuntCompensator.aVRDelay", "ShuntCompensator.sections"]}
    ,
    "switches": {
        "attribute": "Switch.open"
    }
    ,

    "regulators": {
        "attribute": ["RegulatingControl.targetDeadband", "RegulatingControl.targetValue", "TapChanger.initialDelay",
                      "TapChanger.lineDropCompensation", "TapChanger.step", "TapChanger.lineDropR",
                      "TapChanger.lineDropX"]},

    "measurements": {
        "attribute": ["measurement_mrid", "type", "magnitude", "angle", "value"]}
        # ["Discrete", "Analog" ,"Measurement.PowerSystemResource", "Measurement.Terminal",
        #  "Measurement.phases","Measurement.measurementType"]  ## TODO check against points file

}
class DNP3Mapping():
    """ This creates dnp3 input and output points for incoming CIM messages  and model dictionary file respectively."""

    def __init__(self, map_file):
        self.c_ao = 0
        self.c_do = 0
        self.c_ai = 0
        self.c_di = 0
        self.c_var = 0
        self.measurements = dict()
        self.out_json = list()
        self.file_dict = map_file
        self.processor_point_def = PointDefinitions()
        self.outstation = DNP3Outstation('',0,'')

    # create_message_updates is called in new_start_service.py (in the new on_message method).
    def create_message_updates(self, simulation_id, message):
        """ This method creates an atomic "updates" object for any outstation to consume via their .Apply method.
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object

        """
        try:
            message_str = 'received message ' + str(message)[:200]
            builder = asiodnp3.UpdateBuilder()
            #print(message_str)
            json_msg = yaml.safe_load(str(message))
            # self.master.apply_update(opendnp3.Binary(0), 12)

            # self.master.send_direct_operate_command(
            #     opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
            #     5,
            #     command_callback)


            if type(json_msg) != dict:
                raise ValueError(
                    ' is not a json formatted string.'
                    + '\njson_msg = {0}'.format(json_msg))

            # fncs_input_message = {"{}".format(simulation_id): {}}
            #print("measurement_values")
            measurement_values = []
            if "message" in json_msg:
                measurement_values = json_msg["message"]["measurements"]

            # print(json_msg)
            # received message {'command': 'update', 'input': {'simulation_id': '1764973334', 'message': {'timestamp': 1597447649, 'difference_mrid': '5ba5daf7-bf8b-4458-bc23-40ea3fb8078f', 'reverse_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 1}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 1}], 'forward_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 0}]}}}
            if "input" in json_msg.keys():
                #print("input Jeff")

                control_values = json_msg["input"]["message"]["forward_differences"]
                #print(control_values)
                # exit(0)

                ## TODO get command
                ## TODO Dictionionary mapping for master
                ## master = new Master(IP_ADDR, PORT, DNP3_ADDR)
                ## master_dict[object_id] = master
                for command in control_values:
                    #print("command", command)
                    master = self.master_dict[command["object"]]
                    #print(master)
                    for point in master.get_agent().point_definitions.all_points():
                        #print("command", command['object'], command['value'])
                        #print("point",point, point.measurement_id, point.value)
                        # print("y",y)
                        if command.get("object") == point.measurement_id and point.value != command.get("value"):

                            #print("point", point)
                            point.value = command.get("value")
                            #print(opendnp3.Binary(point.value), point.index)
                            # self.master.apply_update(opendnp3.Binary(point.value), point.index)
                            ## TODO Select and operate?
                            if command['value'] == 0:
                                master.send_direct_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON), 5, command_callback)
                            else:
                                master.send_direct_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF), 5, command_callback)
                            # builder.Update(opendnp3.Binary(point.value), point.index)
            else:
                print("No input")

            #print("measurement_values")
            if (measurement_values):

                myPoints = self.outstation.get_agent().point_definitions.all_points()
                netPoints = list(filter(lambda x: "net-" in x.description, myPoints))

                for point in netPoints:
                    ptMeasurements = list(filter(lambda m: m.get("measurement_mrid") in point.measurement_id,
                                                 measurement_values.values()))
                    # print(type(measurement_values.values()))
                    netValue = 0.0

                    for m in ptMeasurements:
                        if "VAR" in point.name:
                            angle = math.radians(m.get("angle"))
                            netValue = netValue + math.sin(angle) * float(m.get("magnitude"))

                        elif "Watts" in point.name:
                            angle = math.radians(m.get("angle"))
                            netValue += math.cos(angle) * float(m.get("magnitude"))

                        else:
                            netValue += float(m.get("magnitude"))

                    point.magnitude = netValue
                    builder.Update(opendnp3.Analog(point.magnitude), point.index)
                    # print("==========", point.magnitude, point.index)
            # Calculate each measurement
            for y in measurement_values:
                # print(self.processor_point_def.points_by_mrid())
                m = measurement_values[y]

                if "magnitude" in m.keys():
                   for point in self.outstation.get_agent().point_definitions.all_points():
                       #print("point",point.name)
                       #print("y",y)
                        if m.get("measurement_mrid") == point.measurement_id:
                            if point.magnitude != float(m.get("magnitude")):
                                point.magnitude = float(m.get("magnitude"))
                                builder.Update(opendnp3.Analog(point.magnitude), point.index)

                            if point.measurement_type == "VA":
                                if "VAR" in point.name:
                                    angle = math.radians(m.get("angle"))
                                    point.magnitude = math.sin(angle) * float(m.get("magnitude"))
                                    builder.Update(opendnp3.Analog(point.magnitude), point.index)

                                if "Watts" in point.name:
                                    angle1 = math.radians(m.get("angle"))
                                    point.magnitude = math.cos(angle1) * float(m.get("magnitude"))
                                    builder.Update(opendnp3.Analog(point.magnitude), point.index)

                                if  "angle" in point.name:
                                    angle2 = math.radians(m.get("angle"))
                                    builder.Update(opendnp3.Analog(angle2), point.index)

                            OesTester.print_solarpanel_output_measurements(point, m)

                elif "value" in m.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if m.get("measurement_mrid") == point.measurement_id and point.value != m.get("value") and point.group != 30:
                            point.value = m.get("value")
                            builder.Update(opendnp3.Binary(point.value), point.index)
                        if m.get("measurement_mrid") == point.measurement_id and point.value != m.get("value") and point.group == 30:
                            point.magnitude = m.get("value")
                            builder.Update(opendnp3.Analog(point.magnitude), point.index)

                        OesTester.print_switch_position(point, m)
                        OesTester.print_voltageregulator_output_measurements(point, m)
            # Return the atomic "updates" object
            print("Updates Created")
            return builder.Build()
        except Exception as e:
            message_str = "An error occurred while trying to translate the  message received" + str(e)


    def assign_val_a(self, data_type, group, variation, index, name, description, measurement_type, measurement_id):
        """ Method is to initialize  parameters to be used for generating  output  points for measurement key values """
        records = dict()  # type: Dict[str, Any]
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        records["magnitude"] = "0"
        self.out_json.append(records)

    def assign_val_d(self, data_type, group, variation, index, name, description, measurement_id, attribute):
        """ This method is to initialize  parameters to be used for generating  output  points for output points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        # records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        records["attribute"] = attribute
        records["value"] = "0"
        self.out_json.append(records)

    def assign_valc(self, data_type, group, variation, index, name, description, measurement_id, attribute):
        """ Method is to initialize  parameters to be used for generating  dnp3 control as Analog/Binary Input points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        # records["measurement_type"] = measurement_type
        records["attribute"] = attribute
        records["measurement_id"] = measurement_id
        self.out_json.append(records)

    def load_json(self, out_json, out_file):
        with open(out_file, 'w') as fp:
            out_dict = dict({'points': out_json})
            json.dump(out_dict, fp, indent=2, sort_keys=True)

    def load_point_def(self, point_def):
        self.processor_point_def = point_def
        
    def load_outstation(self, outstation):
        self.outstation = outstation

    # def load_master(self, master):
    #     self.master = master

    def load_master_dict(self, master_dict):
        self.master_dict = master_dict

    def _create_dnp3_object_map(self):
        """This method creates the points by taking the input data from model dictionary file"""

        feeders = self.file_dict.get("feeders", [])
        measurements = list()
        capacitors = list()
        regulators = list()
        switches = list()
        solarpanels = list()
        batteries = list()
        fuses = list()
        breakers = list()
        reclosers = list()
        energyconsumers = list()

        # Added sorting on name to maintain Index Orders for dnp3 index generation between different containers
        for x in feeders:
            measurements = sorted(x.get("measurements", []), key=lambda x: (x['name'],x['measurementType'],x["phases"]), reverse=True)
            capacitors = sorted(x.get("capacitors", []), key=lambda x: x['name'], reverse=True)
            regulators = sorted(x.get("regulators", []), key=lambda x: x['bankName'], reverse=True)
            switches = sorted(x.get("switches", []), key=lambda x: x['name'], reverse=True)
            solarpanels = sorted(x.get("solarpanels", []), key=lambda x: x['name'], reverse=True)
            batteries = sorted(x.get("batteries", []), key=lambda x: x['name'], reverse=True)
            fuses = sorted(x.get("fuses", []), key=lambda x: x['name'], reverse=True)
            breakers = sorted(x.get("breakers", []), key=lambda x: x['name'], reverse=True)
            reclosers = sorted(x.get("reclosers", []), key=lambda x: x['name'], reverse=True)
            energyconsumers = sorted(x.get("energyconsumers", []), key=lambda x: x['name'], reverse=True)

        # Unique grouping of measurements - GroupBy Name, Type and Connectivity node
        groupByNameTypeConNode = defaultdict(list)
        for m in measurements:
            groupByNameTypeConNode[m['name']+m.get("measurementType")+m.get("ConnectivityNode")].append(m)

        # Create Net Phase DNP3 Points
        for grpM in groupByNameTypeConNode.values():

            if grpM[0]['MeasurementClass'] == "Analog" and grpM[0].get("measurementType") == "VA":
                measurement_type = grpM[0].get("measurementType")
                measurement_id = ""
                for m in grpM:
                    measurement_id += m.get("mRID") + ","

                name1 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-VAR-value'
                name2 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-Watts-value'
                name3 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-VA-value'

                description1 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-VAR" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")
                description2 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-Watts" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")
                description3 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-VA" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")

                self.assign_val_a("AI", 30, 1, self.c_ai, name1, description1, measurement_type, measurement_id)
                self.c_ai += 1
                self.assign_val_a("AI", 30, 1, self.c_ai, name2, description2, measurement_type, measurement_id)
                self.c_ai += 1
                self.assign_val_a("AI", 30, 1, self.c_ai, name3, description3, measurement_type, measurement_id)
                self.c_ai += 1

        # Create Each Phase DNP3 Points
        for m in measurements:
            attribute = attribute_map['regulators']['attribute']
            measurement_type = m.get("measurementType")
            measurement_id = m.get("mRID")
            name= m['name'] + '-' + m['phases']
            description = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + measurement_type + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
            if m['MeasurementClass'] == "Analog":
                self.assign_val_a("AI", 30, 1, self.c_ai, name, description, measurement_type, measurement_id)
                self.c_ai += 1

                if m.get("measurementType") == "VA":
                    measurement_id = m.get("mRID")
                    name1 = m['name'] + '-' + m['phases'] +  '-VAR-value'
                    name2 = m['name'] + '-' + m['phases'] + '-Watts-value'
                    name3 = m['name'] + '-' + m['phases'] + '-angle'

                    description1 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "VAR" + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
                    description2 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "Watt" + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
                    description3 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "angle" + ",ConnectivityNode:" + m.get("ConnectivityNode") + ",SimObject:" + m.get("SimObject")
                    if m['MeasurementClass'] == "Analog":
                        self.assign_val_a("AI", 30, 1, self.c_ai, name1, description1, measurement_type, measurement_id)
                        self.c_ai += 1
                        self.assign_val_a("AI", 30, 1, self.c_ai, name2, description2, measurement_type, measurement_id)
                        self.c_ai += 1
                        self.assign_val_a("AI", 30, 1, self.c_ai, name3, description3, measurement_type, measurement_id)
                        self.c_ai += 1


            elif m['MeasurementClass'] == "Discrete" and  measurement_type == "Pos":
                if "RatioTapChanger" in m['name'] or "reg" in m["SimObject"]:
                    for r in range(0, 7):
                        description = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + attribute[r] + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")

                        self.assign_val_a("AI", 30, 1, self.c_ai, name, description, attribute[r], measurement_id)
                        self.c_ai += 1
                else:
                    self.assign_valc("DI", 1, 2, self.c_di, name, description, measurement_id, measurement_type)
                    self.c_di += 1

        for m in capacitors:
            measurement_id = m.get("mRID")
            cap_attribute = attribute_map['capacitors']['attribute']  # type: List[str]

            for l in range(0, 6):
                # publishing attribute value for capacitors as Bianry/Analog Input points based on phase  attribute
                name = m['name']
                description = "Name:" + m['name'] + ",ConductingEquipment_type:LinearShuntCompensator" + ",Attribute:" + cap_attribute[l]  + ",Phase:" + m['phases']
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, cap_attribute[l])
                self.c_ao += 1
                for p in range(0, len(m['phases'])):
                    name = m['name'] + m['phases'][p]
                    description = "Name:" + m['name'] + ",ConductingEquipment_type:LinearShuntCompensator" + ",controlAttribute:" + cap_attribute[l] + ",Phase:" + m['phases'][p]
                    # description = "Capacitor, " + m['name'] + "," + "phase -" + m['phases'][p] + ", and attribute is - " + cap_attribute[l]
                    self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, cap_attribute[l])
                    self.c_do += 1

        for m in regulators:
            reg_attribute = attribute_map['regulators']['attribute']
            # bank_phase = list(m['bankPhases'])
            for n in range(0, 4):
                measurement_id = m.get("mRID")
                name = m['bankName'] + '-' + m['bankPhases']
                description = "Name:" + m['bankName'] + ",ConductingEquipment_type:RatioTapChanger_Reg" +",Phase:" + m['bankPhases'] + ",Attribute:" + reg_attribute[n]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id[0], reg_attribute[n])
                self.c_ao += 1
            for i in range(4, 7):
                for j in range(0, len(m['bankPhases'])):
                    measurement_id = m.get("mRID")[j]
                    name = m['tankName'][j] + '-' + m['bankPhases'][j]
                    description = "Name:" + m['tankName'][j] + ",ConductingEquipment_type:RatioTapChanger_Reg"+ ",Phase:" + m['bankPhases'][j] + ",controlAttribute:" + reg_attribute[i]
                    self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id,reg_attribute[i])
                    self.c_ao += 1


        for m in solarpanels:
            for k in range(0, len(m['phases'])):
                measurement_id = m.get("mRID")
                name = "Solar" + m['name'] + '-' + m['phases'][k] +  '-Watts-value'
                description = "Type:Solarpanel, Name:" + m['name'] + ",Phase:" + m['phases'] + ",measurementID:" + measurement_id
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1

                name1 = "Solar" + m['name'] + '-' + m['phases'][k] +  '-VAR-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name1, description, measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1

                name2 = "Solar" + m['name'] + '-' + m['phases'][k] +  '-VAR-Net-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name2, description, measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1

                name3 = "Solar"+ m['name'] + '-' + m['phases'][k] +  '-Watts-Net-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name3, description, measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1

        for m in batteries:
            for l in range(0, len(m['phases'])):
                measurement_id = m.get("mRID")
                name = m['name'] + '-' + m['phases'][l] +  '-Watts-value'
                description = "Type:Battery,Name:" + m['name']+ ",Phase:" + m['phases'][l] + ",ConductingEquipment_type:PowerElectronicConnections"
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description,measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1
                name1 = m['name'] + '-' + m['phases'][l] +  '-VAR-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name1, description,measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1

        for m in switches:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][k]
                description = "Name:" + m["name"] + ",ConductingEquipment_type:LoadBreakSwitch" + "Phase:" + phase_value[k] +",controlAttribute:"+switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in fuses:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for l in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][l]
                description = "Name:" + m["name"] + ",Phase:" + phase_value[l] + ",Attribute:" + switch_attribute + ",mRID" + measurement_id
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in breakers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for n in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][n]
                description = "Name:" + m["name"] + ",Phase:" + phase_value[n] + ",ConductingEquipment_type:Breaker" + ",controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in reclosers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for i in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][i]
                description = "Name:" + m["name"] + ",Phase:" + phase_value[i] + ",ConductingEquipment_type:Recloser,"+"controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in energyconsumers:
            measurement_id = m.get("mRID")
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name']+"phase:" + m['phases'][k]
                description = "Name:" + m['name'] + ",ConductingEquipment_type:EnergyConsumer,Phase:" + phase_value[k]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, "EnergyConsumer.p")
                self.c_ao += 1

                name1 = m['name']+"phase:" + m['phases'][k] + "control"
                self.assign_val_d("DO", 12, 1, self.c_do, name1, description, measurement_id, "EnergyConsumer.p")
                self.c_do += 1

        return self.out_json

