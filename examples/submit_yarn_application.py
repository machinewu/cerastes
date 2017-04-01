#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
from cerastes import config
from cerastes import proto_utils
from cerastes.client import YarnRmanApplicationClient
from cerastes.errors import YarnError

cluster_name = "prod"
client_class = YarnRmanApplicationClient

def main():

   cerastes_config = config.CerastesConfig()
   app_client = cerastes_config.get_client(cluster_name, client_class)

   application_name="YARN Cerastes App"

   # Application recources
   executorMemory = 512
   memoryOverhead = 128
   executorCores = 1

   resource = proto_utils.create_resource_proto(memory=executorMemory + memoryOverhead, virtual_cores=executorCores)

   am_container_spec = proto_utils.create_container_context_proto()

   try:
     print "Creating New Application"
     response = app_client.get_new_application()
     application_id = response.application_id

     print "Application Created:"
     print application_id

     print "Submit Application to the Resource Manager"
     app_client.submit_application(application_id=application_id, application_name=application_name, resource=resource, am_container_spec=am_container_spec)
     
     print "Application submitted."
     print "Requesting Application Report:"
     report = app_client.get_application_report(application_id=application_id.id, cluster_timestamp=application_id.cluster_timestamp)

     print report

     print "sleep 20 seconds before killing the job ..."
     time.sleep(20)
     app_client.force_kill_application(application_id=application_id)
   except YarnError:
     raise

if __name__ == '__main__':
  main()
