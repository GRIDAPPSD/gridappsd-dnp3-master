
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
