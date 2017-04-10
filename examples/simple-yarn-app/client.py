#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import sys
import os
from cerastes import config
from cerastes import proto_utils
from cerastes.client import YarnRmanApplicationClient
from cerastes.errors import YarnError

cluster_name = "prod"
client_class = YarnRmanApplicationClient

def main():

    # Fetch configuration variables
    command = sys.argv[1]
    n = int(sys.argv[2])
    # path in hdfs where the master is located
    master_path = sys.argv[3]
    application_name="Simple YARN Cerastes App"

    # Parse Yarn Configuration and create client
    cerastes_config = config.CerastesConfig()
    app_client = cerastes_config.get_client(cluster_name, client_class)

    try:
      print "Creating New Application"
      response = app_client.get_new_application()
      application_id = response.application_id

      print "Application Created:"
      print application_id
    except YarnError:
      raise

    # Setup py file for application master
    #st = os.stat("application_master.py")
    #app_master_recource = proto_utils.create_local_resource_proto(key="application_master.py", size=st[ST_SIZE], timestamp=st[ST_MTIME], type=proto_utils.LOCAL_RESOURCE_TYPE.FILE, visibility=proto_utils.LOCAL_RESOURCE_VISIBILITY.PUBLIC, resource_file=os.path.abspath(master_path))
    app_master_recource = proto_utils.create_local_resource_proto( key="application_master.py",
                                                                   type=proto_utils.LOCAL_RESOURCE_TYPE.FILE,
                                                                   visibility=proto_utils.LOCAL_RESOURCE_VISIBILITY.PUBLIC,
                                                                   resource_file=master_path)
    
    # Set up the container launch context for the application master
    am_command = "/usr/bin/python application_master.py %s %s" % (command, n)
    am_container_spec = proto_utils.create_container_context_proto( commands=[ am_command ], local_resources_map=[app_master_recource])

    # Set up resource type requirements for ApplicationMaster
    executorMemory = 256
    memoryOverhead = 128
    executorCores = 1
    resource = proto_utils.create_resource_proto(memory=executorMemory + memoryOverhead, virtual_cores=executorCores)

    try:
      print "Submit Application to the Resource Manager"
      app_client.submit_application(application_id=application_id, queue="default", application_name=application_name, resource=resource, am_container_spec=am_container_spec)
     
      print "Application submitted."
      
      report = app_client.get_application_report(application_id=application_id.id, cluster_timestamp=application_id.cluster_timestamp)
      app_state = report.application_report.yarn_application_state
      # wait for the application to finish
      while app_state not in [ proto_utils.APPLICATION_STATES.FINISHED, proto_utils.APPLICATION_STATES.KILLED, proto_utils.APPLICATION_STATES.FAILED]:
        time.sleep(10)
        report = app_client.get_application_report(application_id=application_id.id, cluster_timestamp=application_id.cluster_timestamp)
        app_state = report.application_report.yarn_application_state


      print "Application finished with state : " + app_state

    except YarnError:
      raise

if __name__ == '__main__':
  main()
