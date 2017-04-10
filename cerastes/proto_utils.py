#!/usr/bin/python

import cerastes.protobuf.resourcemanager_administration_protocol_pb2 as rm_protocol
import cerastes.protobuf.yarn_server_resourcemanager_service_protos_pb2 as yarn_rm_service_protos
import cerastes.protobuf.applicationclient_protocol_pb2 as application_client_protocol
import cerastes.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import cerastes.protobuf.yarn_protos_pb2 as yarn_protos
import cerastes.protobuf.HAServiceProtocol_pb2 as ha_protocol
import cerastes.protobuf.Security_pb2 as security_protocol
import cerastes.protobuf.application_history_client_pb2 as application_history_client_protocol
import cerastes.protobuf.MRClientProtocol_pb2 as mr_client_protocol
import cerastes.protobuf.mr_protos_pb2 as mr_protos

from cerastes.errors import RpcError, YarnError, AuthorizationException, StandbyError 
from cerastes.controller import SocketRpcController
from cerastes.channel import SocketRpcChannel
from cerastes.utils import SyncServicesList

from google.protobuf import reflection, json_format
from six import add_metaclass
from abc import ABCMeta, abstractmethod
from enum import Enum, IntEnum
from datetime import datetime


class APPLICATION_STATES(IntEnum):
    ACCEPTED = yarn_protos.ACCEPTED
    NEW = yarn_protos.NEW
    NEW_SAVING = yarn_protos.NEW_SAVING
    SUBMITTED = yarn_protos.SUBMITTED
    RUNNING = yarn_protos.RUNNING
    FINISHED = yarn_protos.FINISHED
    KILLED = yarn_protos.KILLED
    FAILED = yarn_protos.FAILED

class APPLICATION_SCOPE(IntEnum):
    ALL = yarn_service_protos.ALL
    VIEWABLE = yarn_service_protos.VIEWABLE
    OWN = yarn_service_protos.OWN

class LOCAL_RESOURCE_TYPE(IntEnum):
    ARCHIVE = yarn_protos.ARCHIVE
    FILE = yarn_protos.FILE
    PATTERN = yarn_protos.PATTERN

class LOCAL_RESOURCE_VISIBILITY(IntEnum):
    PUBLIC = yarn_protos.PUBLIC
    PRIVATE = yarn_protos.PRIVATE
    APPLICATION = yarn_protos.APPLICATION

class APPLICATION_ACCESS_TYPE(IntEnum):
    APPACCESS_VIEW_APP = yarn_protos.APPACCESS_VIEW_APP
    APPACCESS_MODIFY_APP = yarn_protos.APPACCESS_MODIFY_APP

class NODE_STATES(IntEnum):
    NS_NEW = yarn_protos.NS_NEW
    NS_RUNNING = yarn_protos.NS_RUNNING
    NS_UNHEALTHY = yarn_protos.NS_UNHEALTHY
    NS_DECOMMISSIONED = yarn_protos.NS_DECOMMISSIONED
    NS_LOST = yarn_protos.NS_LOST
    NS_REBOOTED = yarn_protos.NS_REBOOTED

class RESERVATION_REQUEST_INTERPRETER(IntEnum):
    R_ANY = yarn_protos.R_ANY
    R_ALL = yarn_protos.R_ALL
    R_ORDER = yarn_protos.R_ORDER
    R_ORDER_NO_GAP = yarn_protos.R_ORDER_NO_GAP

class FINAL_APPLICATION_STATUS(IntEnum):
    APP_UNDEFINED = yarn_protos.APP_UNDEFINED
    APP_SUCCEEDED = yarn_protos.APP_SUCCEEDED
    APP_FAILED = yarn_protos.APP_FAILED
    APP_KILLED = yarn_protos.APP_KILLED

class TASKTYPE(IntEnum):
    MAP = mr_protos.MAP
    REDUCE = mr_protos.REDUCE

def create_jobid_proto(job_id, app_id=None):
    if app_id:
        if not isinstance(app_id, yarn_protos.ApplicationIdProto):
            app_id = create_applicationid_proto(id=app_id)
    return  mr_protos.JobIdProto(id=job_id, app_id=app_id)

def create_taskid_proto(task_id, task_type=None, job_id=None):
    if job_id:
        if not isinstance(job_id, mr_protos.JobIdProto):
            raise YarnError("job_id need to be of type JobIdProto.")

    if task_type:
        if not isinstance(task_type, TASKTYPE):
            raise YarnError("task_type need to be of type TASKTYPE.")

    return  mr_protos.TaskIdProto(id=task_id, job_id=job_id, task_type=task_type)

def create_task_attempt_id_proto(attempt_id, task_id=None):
    if task_id:
        if not isinstance(task_id, mr_protos.TaskIdProto):
            raise YarnError("task_id need to be of type TaskIdProto.")

    return  mr_protos.TaskAttemptIdProto(id=attempt_id, task_id=task_id)

def create_local_resource_proto( key=None, scheme=None, 
                                 host=None, port=None, resource_file=None,
                                 userInfo=None, size=None, timestamp=None,
                                 recource_type=None, visibility=None, pattern=None):

    resource = yarn_protos.LocalResourceProto(scheme=scheme, host=host, port=port, file=resource_file, userInfo=userInfo)
    if recource_type:
        if not isinstance(recource_type, LOCAL_RESOURCE_TYPE):
            raise YarnError("recource_type need to be of type LOCAL_RESOURCE_TYPE.")
    if visibility:
        if not isinstance(visibility, LOCAL_RESOURCE_VISIBILITY):
            raise YarnError("visibility need to be of type LOCAL_RESOURCE_VISIBILITY.")
    local_resource = yarn_protos.LocalResourceProto(resource=resource, size=size, timestamp=timestamp, type=recource_type, visibility=visibility, pattern=pattern)
    return yarn_protos.StringLocalResourceMapProto(key=key, value=local_resource)

def create_service_data_proto(key=None, value=None):
    if value:
        if not isinstance(value, bytes):
            raise YarnError("value need to be of type bytes.")
    return yarn_protos.StringBytesMapProto(key=key, value=value)

def create_environment_proto(key=None, value=None):
    return yarn_protos.StringStringMapProto(key=key, value=value)

def create_application_acl_proto(accessType=None, acl=None):
    if accessType:
        if not isinstance(accessType, APPLICATION_ACCESS_TYPE):
            raise YarnError("accessType need to be of type APPLICATION_ACCESS_TYPE.")
    return yarn_protos.ApplicationACLMapProto(accessType=accessType, acl=acl)

def create_container_context_proto(local_resources_map=None, tokens=None, service_data_map=None, environment_map=None, commands=None, application_ACLs=None):
    if local_resources_map:
        if type(local_resources_map) in (tuple, list):
            for local_resource in local_resources_map:
                if not isinstance(local_resource, yarn_protos.StringLocalResourceMapProto):
                    raise YarnError("local_resources_map need to be a list of StringLocalResourceMapProto.")
        else:
            if isinstance(local_resources_map, yarn_protos.StringLocalResourceMapProto):
                local_resources_map = [local_resources_map]
            else:
                raise YarnError("local_resources_map need to be a list of StringLocalResourceMapProto.")

    if tokens:
        if type(tokens) in (tuple, list):
            for token in tokens:
                if not isinstance(token, bytes):
                    raise YarnError("tokens need to be a list of bytes.")
        else:
            if isinstance(tokens, bytes):
                tokens = [tokens]
            else:
                raise YarnError("tokens need to be a list of bytes.")

    if service_data_map:
        if type(service_data_map) in (tuple, list):
            for service_data in service_data_map:
                if not isinstance(service_data, yarn_protos.StringBytesMapProto):
                    raise YarnError("service_data_map need to be a list of StringBytesMapProto.")
        else:
            if isinstance(service_data_map, yarn_protos.StringBytesMapProto):
                service_data_map = [service_data_map]
            else:
                raise YarnError("service_data_map need to be a list of StringBytesMapProto.")

    if environment_map:
        if type(environment_map) in (tuple, list):
            for environment in environment_map:
                if not isinstance(environment, yarn_protos.StringStringMapProto):
                    raise YarnError("environment_map need to be a list of StringStringMapProto.")
        else:
            if isinstance(environment_map, yarn_protos.StringStringMapProto):
                environment_map = [environment_map]
            else:
                raise YarnError("environment_map need to be a list of StringStringMapProto.")

    if commands:
        if type(commands) in (tuple, list):
            for command in commands:
                if not isinstance(command, str):
                    raise YarnError("commands need to be a list of str.")
        else:
            if isinstance(commands, str):
                commands = [commands]
            else:
                raise YarnError("commands need to be a list of str.")

    if application_ACLs:
        if type(application_ACLs) in (tuple, list):
            for acl in application_ACLs:
                if not isinstance(acl, yarn_protos.ApplicationACLMapProto):
                    raise YarnError("application_ACLs need to be a list of ApplicationACLMapProto.")
        else:
            if isinstance(application_ACLs, yarn_protos.ApplicationACLMapProto):
                application_ACLs = [application_ACLs]
            else:
                raise YarnError("application_ACLs need to be a list of ApplicationACLMapProto.")

    return yarn_protos.ContainerLaunchContextProto( localResources=local_resources_map,
                                                    tokens=tokens, service_data=service_data_map,
                                                    environment=environment_map, command=commands,
                                                    application_ACLs=application_ACLs)

def create_resource_proto(memory=None, virtual_cores=None):
    return yarn_protos.ResourceProto(memory=memory, virtual_cores=virtual_cores)

def create_Log_aggregation_context_proto(include_pattern=None, exclude_pattern=None):
    return yarn_protos.LogAggregationContextProto(include_pattern=include_pattern, exclude_pattern=exclude_pattern)

def create_reservationid_proto(id=None, cluster_timestamp=None):
    return yarn_protos.ReservationIdProto(id=id, cluster_timestamp=cluster_timestamp)

def create_applicationid_proto(id=None, cluster_timestamp=None):
    return yarn_protos.ApplicationIdProto(id=id, cluster_timestamp=cluster_timestamp)

def create_application_attempt_id_proto(application_id=None, attemptId=None):
    if application_id:
        if not isinstance(application_id, yarn_protos.ApplicationIdProto):
            application_id = create_applicationid_proto(id=application_id)
    return yarn_protos.ApplicationAttemptIdProto(application_id=application_id, attemptId=attemptId)

def create_resource_blacklist_request_proto(blacklist_additions=None, blacklist_removals=None):
    return  yarn_protos.ResourceBlacklistRequestProto(blacklist_additions=blacklist_additions, blacklist_removals=blacklist_removals)

def create_container_resource_increase_request_proto(container_id=None, capability=None):
    if capability:
        if not isinstance(capability, yarn_protos.ResourceProto):
            raise YarnError("capability need to be of type ResourceProto.")
    if container_id:
        if not isinstance(container_id, yarn_protos.ContainerIdProto):
            raise YarnError("container_id need to be of type ContainerIdProto.")
    return  yarn_protos.ContainerResourceIncreaseRequestProto(container_id=container_id, capability=capability)

def create_containerid_proto(app_id, app_attempt_id, container_id):
    if app_id:
        if not isinstance(app_id, yarn_protos.ApplicationIdProto):
            app_id = create_applicationid_proto(id=app_id)
    if app_attempt_id:
        if not isinstance(app_attempt_id, yarn_protos.ApplicationAttemptIdProto):
            app_attempt_id = create_application_attempt_id_proto(application_id=app_id, attemptId=app_attempt_id)
    return  yarn_protos.ContainerIdProto(app_id=app_id, app_attempt_id=app_attempt_id, id=container_id)

def create_start_container_request(container_launch_context, container_token=None):
        if container_launch_context:
            if not isinstance(container_launch_context, yarn_protos.ContainerLaunchContextProto):
                raise YarnError("container_launch_context need to be of type ContainerLaunchContextProto.")
        if container_token:
            if not isinstance(container_token, security_protocol.TokenProto):
                raise YarnError("container_token need to be of type TokenProto.")

        return yarn_service_protos.StartContainerRequestProto(container_launch_context=container_launch_context, container_token=container_token)

def create_container_resource_request(priority, resource_name, capability, num_containers, relax_locality, node_label_expression):
    priority_proto = yarn_protos.PriorityProto(priority=priority)
    if capability:
        if not isinstance(capability, yarn_protos.ResourceProto):
            raise YarnError("capability need to be of type ResourceProto.")

    return yarn_protos.ResourceRequestProto( priority=priority_proto, resource_name=resource_name, capability=capability,
                                             num_containers=num_containers, relax_locality=relax_locality, node_label_expression=node_label_expression)

def create_priority_proto(priority=None):
    return yarn_protos.PriorityProto(priority=priority)

def create_token_proto(identifier, password, kind, service):
    return security_protocol.TokenProto(identifier=identifier, password=password, kind=kind, service=service)

def create_reservation_request_proto(capability=None, num_containers=None, concurrency=None, duration=None):
    if capability:
        if not isinstance(capability, yarn_protos.ResourceProto):
            raise YarnError("capability need to be of type ResourceProto.")
    return yarn_protos.ReservationRequestProto(capability=capability, num_containers=num_containers, concurrency=concurrency, duration=duration)

def create_reservation_requests_proto(reservation_resources=None, interpreter=None):
    if reservation_resources:
        if type(reservation_resources) in (tuple, list):
            for reservation in reservation_resources:
               if not isinstance(reservation, yarn_protos.ReservationRequestProto):
                  raise YarnError("reservation_resources need to be a list of ReservationRequestProto.")
        else:
            if isinstance(reservation_resources, yarn_protos.ReservationRequestProto):
                reservation_resources = [reservation_resources]
            else:
                raise YarnError("reservation_resources need to be a list of ReservationRequestProto.")

    if interpreter:
        if not isinstance(interpreter, RESERVATION_REQUEST_INTERPRETER):
            raise YarnError("interpreter need to be of type Enum RESERVATION_REQUEST_INTERPRETER.")
    return yarn_protos.ReservationRequestsProto(reservation_resources=reservation_resources, interpreter=interpreter)

def create_reservation_definition_proto(reservation_requests=None, arrival=None, deadline=None, reservation_name=None):
    if reservation_requests:
        if not isinstance(reservation_requests, yarn_protos.ReservationRequestsProto):
            raise YarnError("reservation_requests need to be of type ReservationRequestsProto.")
    return yarn_protos.ReservationDefinitionProto(reservation_requests=reservation_requests, arrival=arrival, deadline=deadline, reservation_name=reservation_name)
