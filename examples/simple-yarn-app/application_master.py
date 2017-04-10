#!/usr/bin/python
# -*- coding: utf-8 -*-

from cerastes import config
from cerastes import proto_utils
from cerastes.client import YarnRmanApplicationClient, YarnContainerManagerClient
from cerastes.errors import YarnError
import sys
import time

def main():
    
    command = sys.argv[0]
    n = int(sys.argv[1])
    
    # Parse Yarn Configuration and create client
    cerastes_config = config.CerastesConfig()
    am_client = cerastes_config.get_client(cluster_name, YarnApplicationMasterClient)
    
    # register application master
    print "registering Application Master"
    am_client.register_application_master()  
    

    # Make container requests to ResourceManager
    container_requests = []
    capability = proto_utils.create_resource_proto(memory=128, virtual_cores=1)
    
    for i in range(0,n-1):    
      container_ask = proto_utils.create_container_resource_request(priority=0, capability=capability)
      print "Making res-req %s" % i
      container_requests.append(container_ask)
      
    # Obtain allocated containers, launch and check for responses
    response_id = 0
    completed_containers = 0
    while completedContainers < n:
        allocate_response = rmClient.allocate(asks=container_requests, response_id=response_id)
        response_id = response_id + 1
        for container in allocate_response.allocated_containers:
            container_launch_context = proto_utils.create_container_context_proto(commands=[ command ])
            nm_client = YarnContainerManagerClient(host=container.nodeId.host, port=container.nodeId.host, use_sasl=True)
            
            # find associated tokens for secure clusters
            associated_token = None
            for nm_token in allocate_response.nm_tokens:
                if nm_token.nodeId.host == container.nodeId.host and nm_token.nodeId.port == container.nodeId.port:
                    associated_token = nm_token
                    
            nm_client.start_containers( start_container_request = proto_utils.create_start_container_request(container_launch_context=container_launch_context, container_token=associated_token) )
            print "Launching container : %s" + container.id
        
        for status in allocate_response.completed_container_statuses:
            completed_containers = completed_containers +1
            print "Completed container %s" + status.container_id
        time.sleep(10);
            
    # Un-register with ResourceManager
    am_client.finish_application_master(final_application_status=proto_utils.FINAL_APPLICATION_STATUS.SUCCEEDED, diagnostics="", tracking_url="")
    
if __name__ == '__main__':
  main()
