#!/usr/bin/env python3
"""
DNP3 Conversion Script - Ubuntu/Linux Compatible Version
Converts model_dict.json and DNP3_Hypersim_Mapping.csv to conversion dictionaries
"""
import json
import csv
import os
from pathlib import Path
from collections import defaultdict

from importlib_metadata import files

def main():
    # Get script directory for relative paths
    script_dir = Path(__file__).parent
    files_dir = script_dir / "files"
    config_dir = script_dir.parent / "config"

    # Input files
    csv_file = files_dir / "DNP3_Hypersim_Mapping_updated.csv"
    model_file = files_dir / "model_dict_new.json"

    # Output files
    measurement_output = config_dir / "measurement_dict_master.json"
    conversion_output = config_dir / "conversion_dict_master_data.json"

    # Verify input files exist
    if not csv_file.exists():
        print(f"Error: CSV file not found: {csv_file}")
        return
    if not model_file.exists():
        print(f"Error: Model file not found: {model_file}")
        return

    # Create output directory if needed
    config_dir.mkdir(parents=True, exist_ok=True)

    # Initialize conversion dictionary
    conversion_dict = {
        "RTU1": {
            "Analog input": {}
        }
    }
    output_json = {}

    print(f"Reading CSV file: {csv_file}")
    # Read csv file, save measurements mRID
    with open(csv_file, mode='r', encoding='utf-8', newline='') as file:
        reader_csv = csv.reader(file)
        next(reader_csv, None)  # Skip header

        csv_measurements = []
        for row in reader_csv:
            if len(row) >= 4:
                csv_measurements.append(row[3])

    print(f"Found {len(csv_measurements)} measurements in CSV")

    print(f"Reading model file: {model_file}")
    # Read model_dict.json, mapping the data with measurements mRID
    with open(model_file, mode='r', encoding='utf-8') as file:
        reader_json = json.load(file)
        measurements_dataset = reader_json["feeders"][0]["measurements"]

    print(f"Found {len(measurements_dataset)} measurements in model")

    # Process measurements and create output_json
    for measurement_data in measurements_dataset:
        if measurement_data["mRID"] in csv_measurements:
            if measurement_data['measurementType'] == 'PNV':
                eq_name = str(measurement_data["ConductingEquipment_name"])
                meas_type = str(measurement_data["measurementType"])
                phase = str(measurement_data["phases"])

                # Initialize nested structure if needed
                if eq_name not in output_json:
                    output_json[eq_name] = {}
                if meas_type not in output_json[eq_name]:
                    output_json[eq_name][meas_type] = {}

                # Add phase data
                if phase in ['A', 'B', 'C']:
                    if phase not in output_json[eq_name][meas_type]:
                        output_json[eq_name][meas_type][phase] = {
                            'mrid': str(measurement_data["mRID"]),
                            'type': 'angle'
                        }

    # Write measurement dictionary
    print(f"Writing measurement dictionary: {measurement_output}")
    with open(measurement_output, "w", encoding='utf-8') as f:
        json.dump(output_json, f, indent=2)

    # Process CSV again for conversion dictionary
    print(f"Processing conversion dictionary...")
    with open(csv_file, mode='r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header

        for row in reader:
            if len(row) < 4:
                continue

            # Create index entry
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

            # Find matching measurement
            for each_measurement in measurements_dataset:
                if row[3] == each_measurement["mRID"]:
                    # Extract base name (remove extension if present)
                    base_name = row[0].split(".")[0]
                    phase_lower = each_measurement["phases"].lower()
                    each_index["orig_name"] = f"{base_name}_{phase_lower}"
                    each_index["index"] = int(row[2])
                    each_index["CIM name"] = each_measurement["ConductingEquipment_name"]
                    each_index["CIM phase"] = each_measurement["phases"]
                    conversion_dict["RTU1"]["Analog input"][row[2]] = each_index
                    break

    # Write conversion dictionary
    analog_count = len(conversion_dict["RTU1"]["Analog input"])
    print(f"Created {analog_count} analog input mappings")
    print(f"Writing conversion dictionary: {conversion_output}")

    with open(conversion_output, 'w', encoding='utf-8') as f:
        json.dump(conversion_dict, f, indent=2)

    print("\n=== Conversion Complete ===")
    print(f"Analog inputs mapped: {analog_count}")
    print(f"Output files created:")
    print(f"  - {measurement_output}")
    print(f"  - {conversion_output}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
