import json
import csv
from collections import defaultdict
if __name__ == "__main__":
    
    conversion_dict = {
  "RTU1": {
    "Analog input": {
    }
  }
}
    output_json = {}

    # read csv file, save measurements mRID
    with open("./files/DNP3_Hypersim_Mapping.csv", mode='r') as file:
        reader_csv = csv.reader(file)
        next(reader_csv, None)

        csv_measurements = []
        for row in reader_csv:
            csv_measurements.append(row[3])
    #print(csv_measurements)

    # read model_dict.json, mapping the data with measurements mRID
    with open("./files/model_dict.json", mode='r') as file:
        reader_json = json.load(file)
        # measurements_dataset = reader_json["feeders"][0]["measurements"]
        measurements_dataset = reader_json["feeders"][0]["measurements"]
        #print(measurements_dataset)
   
       #model_line_dict[name] = {}
   
        for measurement_data in measurements_dataset:
            if measurement_data["mRID"] in csv_measurements:
                if measurement_data['measurementType'] == 'PNV':

                        #if measurement_data['phases'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] = {}

                        output_json[str(measurement_data["ConductingEquipment_name"])] = {}
                        #
                        # # records[str(measurement_data["ConductingEquipment_name"])] = i
                        output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])] = {}
                        # # phases
                        if measurement_data['phases'] == 'A':
                            if measurement_data['phases'] not in output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])]:
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])] = {}
                                # mrid and type
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['mrid'] = str(measurement_data["mRID"])
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['type'] = 'angle'
                        elif measurement_data['phases'] == 'B':
                            if measurement_data['phases'] not in output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])]:
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])] = {}
                                # mrid and type
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['mrid'] = str(measurement_data["mRID"])
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['type'] = 'angle'
                        elif measurement_data['phases'] == 'C':
                            if measurement_data['phases'] not in output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])]:
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])] = {}
                                # mrid and type
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['mrid'] = str(measurement_data["mRID"])
                                output_json[str(measurement_data["ConductingEquipment_name"])][str(measurement_data["measurementType"])][str(measurement_data["phases"])]['type'] = 'angle'

    with open("../config/measurement_dict_master.json", "w") as f:
        #out_dict = dict({'points': output_json})
        json.dump(output_json, f, indent=2)
   
       # print((measurements))
    with open("./files/DNP3_Hypersim_Mapping.csv", mode='r') as file:
        reader = csv.reader(file)
        next(reader, None)
        for row in reader:
            # print(row)
            each_index = {
                "orig_name": "",
                "index": "",
                "Multiplier": 1,
                "CIM attribute": "magnitude",
                "CIM units": "PNV",
                "CIM Variable": "V",
                "CIM type": "PNV",
                "CIM name": "",
                "CIM phase": ""
              }
            for each_measurement in measurements_dataset:
                if row[3] == each_measurement["mRID"]:
                    each_index["orig_name"] = row[0].split(".")[0] + "_" + each_measurement["phases"].lower()
                    each_index["index"] = int(row[2])
                    each_index["CIM name"] = each_measurement["ConductingEquipment_name"]
                    each_index["CIM phase"] = each_measurement["phases"]
                    conversion_dict["RTU1"]["Analog input"][row[2]] = each_index

        print(len(conversion_dict["RTU1"]["Analog input"]))
        with open('../config/conversion_dict_master_data.json', 'w') as f:
            json.dump(conversion_dict, f, indent=2)