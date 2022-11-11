import json
import time
import os
import argparse
from gridappsd import GOSS
import random






goss_sim = "goss.gridappsd.process.request.simulation"
test_topic = 'goss.gridappsd.test'
responseQueueTopic = '/temp-queue/response-queue'
goss_simulation_status_topic = '/topic/goss.gridappsd/simulation/status/'
#1375423200
# def _startTest(username,password,gossServer='localhost',stompPort='61613', simulationID=1234, rulePort=5000, topic="input"):
def start_test(feeder='_C1C3E687-6FFD-C753-582B-632A27E28507', duration=60):
    req_template = {"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","Line_name":feeder},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee13ochre","simulator":"GridLAB-D","start_time":1375459200,"run_realtime":True,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":0.0,"p_fraction":1.0,"randomize_zipload_fractions":False,"use_houses":False},"simulation_broker_port":52798,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[]},"simulation_request_type":"NEW"}
    req_template['simulation_config']['duration'] = duration
    req_template['power_system_config']['Line_name'] = feeder  # IEEE 123
    # req_template["service_configs"] = [{"id": "OCHRE", "user_options": {}}]
    # req_template["application_config"]["applications"][0]['name'] = app_name

    simCfg13pv = json.dumps(req_template)
 ###   print('I am using feeder: ' + feeder)
    ###print("request")
  ###  print(json.dumps(req_template,indent=2))
    username = "app_user"
    password = "1234App"
    goss = GOSS(username=username, password=password)
    goss.connect()

    simulationId = goss.get_response(goss_sim, simCfg13pv, timeout=10)
   ### print('simulation id created', simulationId)

    return(simulationId['simulationId'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input", required=False)
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000, required=False)
    parser.add_argument("-i", "--id", type=int, help="simulation id", required=False)
    # parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    # parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()

    # _startTest('system','manager',gossServer='127.0.0.1',stompPort='61613', simulationID=args.id, rulePort=args.port, topic=args.topic)
    start_test()





























































 # python /usr/src/gridappsd-sample/sample_app/runsample.py 1201658254 {"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","Line_name":"_C1C3E687-6FFD-C753-582B-632A27E28507"},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1248156000,"run_realtime":true,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":52798,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"sample_app","config_string":""}]},"simulation_request_type":"NEW"}
