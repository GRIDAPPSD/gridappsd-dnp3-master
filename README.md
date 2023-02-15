
# GridAPPS-D DNP3 Master Service 

The GridAPPS-D DNP3 Master Service acts as a bridge between GridAPPS-D platform and DNP3 Masters. The service connects with the DNP3 Masters and collects binary and analog output. It then converts them to CIM compliant message and publishes on Field Interface Manager's output topic. This message can then be received by any GridAPPS-D complaint application. The service also subscribes to control commands coming from GridAPPS-D applications and forwards them to DNP3 Masters after conversion.

## Running the service
By default the service runs with IEEE123 model using the configurations files in dnp3-master/service/config. For more see Config heading.
```
 python3 start_service.py 'RTU1' 
```

## Workflow
`Needs to he updated`

1. Lookup device ip and port information in device_ip_port_config_all.json file.
2. The SOEHandler in the master.py will handle the indexes to CIM MRID mapping and conversion.
3. CIMProcessor in CIMPro.py will handle the Analog Output data transfer
4. Add or delete Analog Input (AI) or Analog Output (AO) in IEEE123_RTAC_AI_AO.xlsx file according to your application
5. Run Conversion_dict_IEEE123.py to generate both conversion_dict_master.json and model_line_dict_master.json
6. Run start_service.py (Check IP adrees of outstation, Port number, DNP3 address)

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

