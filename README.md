
# Master DNP3 details

The start_service.py will start all the masters and collect the built CIM message from each master.SOEHandler and send to the FIM topic.
1. Lookup device ip and port information in device_ip_port_config_all.json file.
2. The SOEHandler in the master.py will handle the indexes to CIM MRID mapping and conversion.
3. CIMProcessor in CIMPro.py will handle the Analog Output data transfer
4. Add or delete Analog Input (AI) or Analog Output (AO) in IEEE123_RTAC_AI_AO.xlsx file according to your application
5. Run Conversion_dict_IEEE123.py to generate both conversion_dict_master.json and model_line_dict_master.json
6. Check both json file paths inside Master_NREl_Start.py inside Master_GridAPPSD_files folder
7. Run start_service.py (Check IP adrees of outstation, Port number, DNP3 address)

## Terminal 
```
 python3 start_service.py 'RTU1' 
```

## Config

The dnp3-master/service/config directiry contains the configuration files needed by the service as an input:

**1. conversion_dict_master_data.json**

**2. device_ip_port_config.json**

**3. measurement_dict_master.json**

## Scripts

The dnp3-master/service/scripts directory contains time saving python scripts that automate config files creations.

**1. automation_mapping.py**

It reads the model dictionary file and an Excel file containing DNP3 point names with indexes to generate mapping between CIM and DNP3 objects as well as measurements. The scripts currently works with following input and output files:

Input: dn3-master/scripts/files/model_dict.json 

Input: dnp3-master/scripts/files/IEEE123_RTAC_AI_AO_Mod2.xlsx

Output: dnp3-master/config/converstion_dict_master.json

Output: dnp3-master/config/measurement_dict_master.json

**2. DNP3-Mapper-conv.py**

It reads the model dictionary file and an Excel file containing DNP3 point names with indexes and **MRIDs** to generate mapping between CIM and DNP3 objects as well as measurements. The scripts currently works with following input and output files:

Input: dn3-master/scripts/files/model_dict.json 

Input: dnp3-master/scripts/files/DNP3_Hypersim_Mapping.xlsx

Output: dnp3-master/config/converstion_dict_master.json

Output: dnp3-master/config/measurement_dict_master.json

