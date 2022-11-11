from dnp3.points import (
    PointArray, PointDefinitions, PointDefinition, DNP3Exception, POINT_TYPE_ANALOG_INPUT, POINT_TYPE_BINARY_INPUT
)

test_switch = False
test_solarpanel = False
test_voltageregulator = True

class OesTester():

    @staticmethod 
    def print_switch_position(point: PointDefinition, m:dict):
        if not test_switch: 
            return
        
        if "LoadBreakSwitch" in point.name:
            print("--------------------------------")
            print("Description:", point.description)
            print("POS:", point.value)
            print("MRID:",  m.get("measurement_mrid"))
            print("--------------------------------")

    @staticmethod
    def print_solarpanel_output_measurements(point: PointDefinition, m: dict):
        if not test_solarpanel: 
            return

        if "VAR" not in point.name and "Watts" not in point.name and "angle" not in point.name:
            if "PhotovoltaicUnit" in point.name or "pv_pvmtr" in point.description or "pv1" in point.description :
                print("--------------------------------")
                print("Type:", point.measurement_type)
                print("Description:", point.description)
                print("Value:", point.value)
                print("Magnitude:", point.magnitude)
                print("MRID:", point.measurement_id)
                print("--------------------------------")

    @staticmethod
    def print_voltageregulator_output_measurements(point: PointDefinition, m: dict):
        if not test_voltageregulator: 
            return
        #or "vr_ld" in point.description.lower()
        if "feeder" in point.description.lower() : 
            print("--------------------------------")
            print("Type:", point.measurement_type)
            print("Name:", point.description)
            print("Description:", point.description)
            print("Value:", point.value)
            print("Index:", point.index)
            print("Group:", point.group)
            print("Magnitude:", point.magnitude)
            print("MRID:", point.measurement_id)
            print("--------------------------------")