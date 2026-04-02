import csv
import json
from pathlib import Path

# File paths
dir_path = Path(__file__).parent
csv_path = dir_path / "files/DNP3_Hypersim_Mapping.csv"
json_path = dir_path / "files/model_dict_new.json"
output_path = dir_path / "files/DNP3_Hypersim_Mapping_updated.csv"

# Load model_dict_new.json
with open(json_path, "r") as jf:
    model = json.load(jf)

# Build a lookup for measurements: (equipment, phase, voltage type) -> (mRID, ConductingEquipment_mRID)
measurements = model["feeders"][0]["measurements"]
lookup = {}
for m in measurements:
    eq_name = m.get("ConductingEquipment_name", "").lower()
    phase = m.get("phases", "").upper()
    mtype = m.get("measurementType", "").upper()
    key = (eq_name, phase, mtype)
    lookup[key] = (m["mRID"], m["ConductingEquipment_mRID"])

def parse_dnp3point(dnp3point):
    # Example: c83_Vrms.o1 or l10_Vrms.o2
    # Extract equipment and voltage type
    if "_" in dnp3point:
        eq, rest = dnp3point.split("_", 1)
        # Try to infer voltage type from rest (e.g., Vrms)
        vtype = "V" if "V" in rest.upper() else ""
        return eq.lower(), vtype
    return dnp3point.lower(), ""

# Read and update CSV
with open(csv_path, newline="") as cf, open(output_path, "w", newline="") as outf:
    reader = csv.DictReader(cf)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outf, fieldnames=fieldnames)
    writer.writeheader()
    for row in reader:
        eq, vtype = parse_dnp3point(row["DNP3point"])
        phase = row["phase"].upper()
        # Try voltage type mapping: Vrms -> PNV, etc.
        mtype = "PNV" if vtype == "V" else ""
        key = (eq, phase, mtype)
        if key in lookup:
            row["Measurement mRID"], row["ConductingEquipment"] = lookup[key]
        writer.writerow(row)

print(f"Updated CSV written to {output_path}")
