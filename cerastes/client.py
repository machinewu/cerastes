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
import cerastes.protobuf.HSAdminRefreshProtocol_pb2 as hs_admin_protocol
import cerastes.protobuf.mr_protos_pb2 as mr_protos
import cerastes.protobuf.applicationmaster_protocol_pb2 as applicationmaster_protocol
import cerastes.protobuf.containermanagement_protocol_pb2 as container_management_protocol
import cerastes.proto_utils as proto_utils

from cerastes.errors import RpcError, YarnError, AuthorizationException, StandbyError 
from cerastes.controller import SocketRpcController
from cerastes.channel import SocketRpcChannel
from cerastes.utils import SyncServicesList

from google.protobuf import reflection, json_format
from six import add_metaclass
from abc import ABCMeta, abstractmethod
from enum import Enum, IntEnum
from datetime import datetime

import re
import logging as lg

log = lg.getLogger(__name__)
DEFAULT_YARN_PROTOCOL_VERSION=9

class _RpcHandler(object):

  def __init__(self, service_stub_class, context_protocol, **kwargs):
    self.service_stub_class = service_stub_class
    self.context_protocol = context_protocol
    self.kwargs = kwargs

  def __call__(self):
    pass # make pylint happy

  def get_handler(self, method):

    def rpc_handler(client, strict=True, **params):
        """Wrapper function."""
        self.method_desc = self.service_stub_class.GetDescriptor().methods_by_name[method]

        rpc_executor = self.service_stub_class.__dict__[self.method_desc.name]
        controller = SocketRpcController()
        req_class = reflection.MakeClass(self.method_desc.input_type)
 
        try:
            request = req_class(**params)
        except AttributeError as ex:
            raise YarnError("Error creating Request class %s : %s" % (req_class, str(ex)))

        '''
        request = req_class()
        for key in params:
            # Assignment to repeated fields not allowed, need to extend
            if isinstance(params[key], list):
                getattr(request, key).extend(params[key])
            else:
                try:
                    setattr(request, key, params[key])
                except AttributeError as ex:
                    raise YarnError("Error initializing Request class %s attribute %s : %s" % (req_class, key, str(ex)))
        '''

        try:
            response = client._call(rpc_executor, controller, request)
            return response
        # parse the error message and return the corresponding exception
        except RpcError as e:
            if "AuthorizationException" in " ".join([e.class_name, e.message]) or "AccessControlException" in " ".join([e.class_name, e.message]):
                raise AuthorizationException(str(e))
            elif "StandbyException" in " ".join([e.class_name, e.message]):
                raise StandbyError(str(e))
            else:
                raise YarnError(str(e)) 
    
    return rpc_handler

# Thanks Matt for the nice piece of code
# inherit ABCMeta to make YarnClient abstract
class _ClientType(ABCMeta):
  pattern = re.compile(r'_|\d')
  def __new__(mcs, name, bases, attrs):
    for key, value in attrs.items():
      if isinstance(value, _RpcHandler):
        attrs[key] = value.get_handler(mcs.pattern.sub('', key))
    client = super(_ClientType, mcs).__new__(mcs, name, bases, attrs)
    return client

# for python 2 and 3 compatibility
@add_metaclass(_ClientType)
class YarnClient(object):
    """
     Abstract RPC Client
     An Abstract implementation of an RPC service client. A client is defined by a protocol, a stub class
     and a set of wrapper functions around the protocol methods.
    """

    def __init__(self, host, port, version=DEFAULT_YARN_PROTOCOL_VERSION, effective_user=None, use_sasl=False, yarn_rm_principal=None,
                 sock_connect_timeout=10000, sock_request_timeout=10000):
        '''
        :param host: Hostname or IP address of the ResourceManager
        :type host: string
        :param port: RPC Port of the ResourceManager
        :type port: int
        :param version: What hadoop protocol version should be used (default: 9)
        :type version: int
        '''
        if version < 9:
            raise YarnError("Only protocol versions >= 9 supported")

        self.host = host
        self.port = port

        # Setup the RPC channel
        self.channel = SocketRpcChannel(host=self.host, port=self.port, version=version,
                                        context_protocol=self.service_protocol, effective_user=effective_user,
                                        use_sasl=use_sasl, krb_principal=yarn_rm_principal,
                                        sock_connect_timeout=sock_connect_timeout,
                                        sock_request_timeout=sock_request_timeout)

        self.service = self.service_stub(self.channel)

        log.debug("Created client for %s:%s", host, port)

    def _call(self, executor, controller, request):
        response =  executor(self.service, controller, request)
        return response

class YarnFailoverClient(YarnClient):
    """
     Abstract Yarn Failover Client. This client attempts requests to a service until a standby error is
     received then switch to the second service.
    """

    def __init__(self, services, version=DEFAULT_YARN_PROTOCOL_VERSION, effective_user=None, use_sasl=False, yarn_rm_principal=None,
                 sock_connect_timeout=10000, sock_request_timeout=10000):

        if version < 9:
            raise YarnError("Only protocol versions >= 9 supported")

        self.services = services
        self.sync_hosts_list = SyncServicesList(services)
        self.version = version
        self.effective_user = effective_user
        self.use_sasl = use_sasl
        self.yarn_rm_principal = yarn_rm_principal
        self.sock_connect_timeout = sock_connect_timeout
        self.sock_request_timeout = sock_request_timeout

        self.service_cache = {}

        self.current_service = self.services[0]
        self.service = self.create_service_stub(services[0]['host'],services[0]['port'])
        self.service_cache[services[0]['host']] = self.service

    def create_service_stub(self, host, port):
        # Setup the RPC channel
        channel = SocketRpcChannel(host=host, port=port, version=self.version,
                                   context_protocol=self.service_protocol, effective_user=self.effective_user,
                                   use_sasl=self.use_sasl, krb_principal=self.yarn_rm_principal,
                                   sock_connect_timeout=self.sock_connect_timeout,
                                   sock_request_timeout=self.sock_request_timeout)

        return self.service_stub(channel)
    
    # called on standby exception
    def switch_active_service(self):
        # find the next service
        self.current_service = self.sync_hosts_list.switch_active_host(self.current_service)
        if self.current_service['host'] in self.service_cache:
            return self.service_cache[self.current_service['host']]
        else:
            self.service = self.create_service_stub(self.current_service['host'],self.current_service['port'])
            self.service_cache[self.current_service['host']] = self.service

    def _call(self, executor, controller, request):
        max_attemps = self.sync_hosts_list.get_host_count()
        attempt = 1
        while attempt <= max_attemps:
          try:
            response =  executor(self.service, controller, request)
            return response
          except RpcError as e:
            if "StandbyException" in " ".join([e.class_name, e.message]):
                self.switch_active_service()
                attempt += 1
                pass
            else:
                raise e

        raise StandbyError('Could not find any active host.')

class YarnAdminClient(YarnClient):
    '''
      Yarn Resource Manager administration client. Implements the list of tasks that need to be performed by Yarn
      Cluster administrator.
      
      Administration Tasks are implemented in Yarn via a separate interface, this is to make sure that
      administration requests don’t get starved by the regular users’ requests and to give the operators’ commands
      a higher priority.
      
      The Yarn administration port is typically 8033.
    '''

    service_protocol = "org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB"
    service_stub = rm_protocol.ResourceManagerAdministrationProtocolService_Stub

    _getGroupsForUser = _RpcHandler( service_stub, service_protocol )
    _refreshServiceAcls = _RpcHandler( service_stub, service_protocol )
    _refreshAdminAcls = _RpcHandler( service_stub, service_protocol )
    _refreshNodes = _RpcHandler( service_stub, service_protocol )
    _refreshQueues = _RpcHandler( service_stub, service_protocol )
    _refreshSuperUserGroupsConfiguration = _RpcHandler( service_stub, service_protocol )
    _refreshUserToGroupsMappings = _RpcHandler( service_stub, service_protocol )
    _updateNodeResource = _RpcHandler( service_stub, service_protocol )
    _addToClusterNodeLabels = _RpcHandler( service_stub, service_protocol )
    _removeFromClusterNodeLabels = _RpcHandler( service_stub, service_protocol )
    _replaceLabelsOnNodes = _RpcHandler( service_stub, service_protocol )

    def refresh_service_acls(self):
        response = self._refreshServiceAcls()
        return True

    def refresh_admin_acls(self):
        response = self._refreshAdminAcls()
        return True

    def refresh_nodes(self):
        response = self._refreshNodes()
        return True

    def refresh_queues(self):
        response = self._refreshQueues()
        return True

    def refresh_super_user_groups_configuration(self):
        response = self._refreshSuperUserGroupsConfiguration()
        return True

    def refresh_user_to_groups_mappings(self):
        response = self._refreshUserToGroupsMappings()
        return True

    def update_node_resource(self, node_resource_map):
        #TODO
        return False

    def add_to_cluster_node_labels(self, nodeLabels):
        if not isinstance(nodeLabels, list):
           raise YarnError("Add To Cluster Node Labels expect array of strings argument")

        response = self._addToClusterNodeLabels(nodeLabels=nodeLabels)
        return True

    def remove_from_cluster_node_labels(self, labels):
        response = self._removeFromClusterNodeLabels(nodeLabels=labels)
        return True

    def replace_labels_on_nodes(self, nodeToLabels):
        #TODO
        return False

    def get_groups_for_user(self, user):
        response = self._getGroupsForUser(user=user)
        if response:
            return [ group for group in response.groups ]
        else:
            return []

class YarnHARMClient(YarnClient):

    service_protocol = "org.apache.hadoop.ha.HAServiceProtocol"
    service_stub = ha_protocol.HAServiceProtocolService_Stub

    _getServiceStatus = _RpcHandler( service_stub, service_protocol )
    _monitorHealth = _RpcHandler( service_stub, service_protocol ) 
    _transitionToStandby = _RpcHandler( service_stub, service_protocol )
    _transitionToActive = _RpcHandler( service_stub, service_protocol )

    class REQUEST_SOURCE(IntEnum):
        REQUEST_BY_USER = ha_protocol.REQUEST_BY_USER
        REQUEST_BY_USER_FORCED = ha_protocol.REQUEST_BY_USER_FORCED
        REQUEST_BY_ZKFC = ha_protocol.REQUEST_BY_ZKFC

    def get_service_status(self):
        response = self._getServiceStatus()
        if response:
            return json_format.MessageToDict(response)
        else:
            return []

    def monitor_health(self):
       # raise an error if there is something wrong
       response = self._monitorHealth()
       return True

    def transition_to_standby(self, source):
       if not isinstance(source, self.REQUEST_SOURCE):
           raise YarnError("scope need to be REQUEST_SOURCE type.")

       reqInfo = ha_protocol.HAStateChangeRequestInfoProto(reqSource=source)
       response = self._transitionToStandby(reqInfo=reqInfo)
       return True

    def transition_to_active(self, source):
       if not isinstance(source, self.REQUEST_SOURCE):
           raise YarnError("scope need to be REQUEST_SOURCE type.")

       reqInfo = ha_protocol.HAStateChangeRequestInfoProto(reqSource=source)
       response = self._transitionToActive(reqInfo=reqInfo)
       return True

class YarnAdminHAClient(YarnAdminClient):
    """
      Yarn Resource Manager HA Administration client.
    """
    def __init__(self, services, version=DEFAULT_YARN_PROTOCOL_VERSION, effective_user=None, use_sasl=False, yarn_rm_principal=None,
                 sock_connect_timeout=10000, sock_request_timeout=10000):

        if version < 9:
            raise YarnError("Only protocol versions >= 9 supported")

        if not isinstance(services, list):
            raise YarnError("Yarn RM Admin Client is valid only when HA is active, services need to be a list of  rm host and port dicts")
        elif len(services) != 2:
            raise YarnError("Yarn RM Admin Client is valid only when HA is active, services need to be a list of two rm host and port dicts")

        self.rm_one_admin_client = YarnAdminClient( host=services[0]['host'], port=services[0]['port'], version=version,
                                                    effective_user=effective_user,
                                                    use_sasl=use_sasl, yarn_rm_principal=yarn_rm_principal,
                                                    sock_connect_timeout=sock_connect_timeout,
                                                    sock_request_timeout=sock_request_timeout)

        self.rm_two_admin_client = YarnAdminClient( host=services[1]['host'], port=services[1]['port'], version=version,
                                                    effective_user=effective_user,
                                                    use_sasl=use_sasl, yarn_rm_principal=yarn_rm_principal,
                                                    sock_connect_timeout=sock_connect_timeout,
                                                    sock_request_timeout=sock_request_timeout)

        self.rm_one_ha_client = YarnHARMClient( host=services[0]['host'], port=services[0]['port'], version=version,
                                                effective_user=effective_user,
                                                use_sasl=use_sasl, yarn_rm_principal=yarn_rm_principal,
                                                sock_connect_timeout=sock_connect_timeout,
                                                sock_request_timeout=sock_request_timeout)

        self.rm_two_ha_client = YarnHARMClient( host=services[1]['host'], port=services[1]['port'], version=version,
                                                effective_user=effective_user,
                                                use_sasl=use_sasl, yarn_rm_principal=yarn_rm_principal,
                                                sock_connect_timeout=sock_connect_timeout,
                                                sock_request_timeout=sock_request_timeout)

        self.services_map = [
                             {
                               'host': services[0]['host'],
                               'port': services[0]['port'],
                               'client': self.rm_one_admin_client,
                               'ha_client': self.rm_one_ha_client
                             },
                             {
                               'host': services[1]['host'],
                               'port': services[1]['port'],
                               'client': self.rm_two_admin_client,
                               'ha_client': self.rm_two_ha_client
                             }
                            ]

    def get_active_rm_service(self):
        for svr in self.services_map:
           if svr['ha_client'].get_service_status()['state'] == "ACTIVE":
              return svr
        raise YarnError("Could not find any active RM server.")

    def get_active_rm_host(self):
        return self.get_active_rm_service()['host']

    def get_active_rm_client(self):
        return self.get_active_rm_service()['client']

    def get_active_ha_handler(self):
        return self.get_active_rm_service()['ha_client']

    def get_standby_rm_service(self):
        for svr in self.services_map:
           if svr['ha_client'].get_service_status()['state'] == "STANDBY":
              return svr
        raise YarnError("Could not find any active RM server.")

    def get_standby_rm_host(self):
        return self.get_standby_rm_service()['host']

    def get_standby_rm_client(self):
        return self.get_standby_rm_service()['client']

    def get_standby_ha_handler(self):
        return self.get_standby_rm_service()['ha_client']

    def explicit_failover(self,force=False):
        ha_sb_client = self.get_standby_rm_service()['ha_client']
        ha_active_client = self.get_active_rm_service()['ha_client']
        if force:
           state = YarnHARMClient.REQUEST_SOURCE.REQUEST_BY_USER_FORCED
        else:
           state = YarnHARMClient.REQUEST_SOURCE.REQUEST_BY_USER
        ha_active_client.transition_to_standby(state)
        ha_sb_client.transition_to_active(state)
        return True

    def _call(self, executor, controller, request):
        active_client = self.get_active_rm_client()
        response =  executor(active_client.service, controller, request)
        return response

class MRAdminClient(YarnClient):

    service_protocol = "org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol"
    service_stub = hs_admin_protocol.HSAdminRefreshProtocolService_Stub

    _refreshAdminAcls = _RpcHandler( service_stub, service_protocol )
    _refreshLoadedJobCache = _RpcHandler( service_stub, service_protocol )
    _refreshJobRetentionSettings = _RpcHandler( service_stub, service_protocol )
    _refreshLogRetentionSettings = _RpcHandler( service_stub, service_protocol )

    def refresh_log_retention_settings(self):
        response = self._refreshLogRetentionSettings()

    def refresh_job_retention_settings(self):
        response = self._refreshJobRetentionSettings()

    def refresh_admin_acls(self):
        response = self._refreshAdminAcls()

    def refresh_loaded_job_cache(self):
        response = self._refreshLoadedJobCache()

class MRClient(YarnClient):
    """
      Client for the Map Reduce Job History server.
    """

    service_protocol = "org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB"
    service_stub = mr_client_protocol.MRClientProtocolService_Stub

    _getJobReport = _RpcHandler( service_stub, service_protocol )
    _getTaskReport = _RpcHandler( service_stub, service_protocol )
    _getTaskAttemptReport = _RpcHandler( service_stub, service_protocol )
    _getCounters = _RpcHandler( service_stub, service_protocol )
    _getDelegationToken = _RpcHandler( service_stub, service_protocol )
    _renewDelegationToken = _RpcHandler( service_stub, service_protocol )
    _cancelDelegationToken = _RpcHandler( service_stub, service_protocol )
    _getTaskAttemptCompletionEvents = _RpcHandler( service_stub, service_protocol )
    _getTaskReports = _RpcHandler( service_stub, service_protocol )
    _getDiagnostics = _RpcHandler( service_stub, service_protocol )
    _killJob = _RpcHandler( service_stub, service_protocol )
    _killTask = _RpcHandler( service_stub, service_protocol )
    _killTaskAttempt = _RpcHandler( service_stub, service_protocol )
    _failTaskAttempt = _RpcHandler( service_stub, service_protocol )

    ''' Kill Functions '''

    def fail_task_attempt(self, attempt_id, task_id=None):
        task_attempt = proto_utils.create_task_attempt_id_proto(attempt_id=attempt_id, task_id=task_id)
        response = self._failTaskAttempt(task_attempt_id=task_attempt)

    def kill_task_attempt(self, attempt_id, task_id=None):
        task_attempt = proto_utils.create_task_attempt_id_proto(attempt_id=attempt_id, task_id=task_id)
        response = self._killTaskAttempt(task_attempt_id=task_attempt)

    def kill_job(self, job_id, app_id=None):
        job = proto_utils.create_jobid_proto(job_id=job_id,app_id=app_id)
        response = self._killJob(job_id=job)

    def kill_task(self, task_id, job_id=None, task_type=None):
        task = proto_utils.create_taskid_proto(task_id=task_id, task_type=task_type, job_id=job_id)
        response = self._killTask(task_id=task)

    ''' Diagnostics Functions '''

    def get_diagnostics(self, attempt_id, task_id=None):
        task_attempt = proto_utils.create_task_attempt_id_proto(attempt_id=attempt_id, task_id=task_id)
        response = self._getDiagnostics(task_attempt_id=task_attempt)
        if response:
            return [ json_format.MessageToDict(diagnostic) for diagnostic in response.diagnostics ]
        else:
            return []

    def get_task_attempt_completion_events(self, job_id, from_event_id=None, max_events=None):
        if job_id:
            if not isinstance(job_id, mr_protos.JobIdProto):
                raise YarnError("job_id need to be of type JobIdProto.")
        response = self._getTaskAttemptCompletionEvents(job_id=job_id, from_event_id=from_event_id, max_events=max_events)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_counters(self, job_id, app_id=None):
        job = proto_utils.create_jobid_proto(job_id=job_id,app_id=app_id)
        response = self._getCounters(job_id=job)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_job_report(self, job_id=None, app_id=None):
        job = proto_utils.create_jobid_proto(job_id=job_id,app_id=app_id)
        response = self._getJobReport(job_id=job)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_task_report(self, task_id, job_id=None, task_type=None):
        task = proto_utils.create_taskid_proto(task_id=task_id, task_type=task_type, job_id=job_id)
        response = self._getTaskReport(task_id=task)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_task_reports(self, job_id, task_type=None):
        if job_id:
            if not isinstance(job_id, mr_protos.JobIdProto):
                raise YarnError("job_id need to be of type JobIdProto.")
        if task_type:
            if not isinstance(task_type, proto_utils.TASKTYPE):
                raise YarnError("task_type need to be of type TASKTYPE.")
        response = self._getTaskReports(job_id=job_id, task_type=task_type)
        if response:
            return [ json_format.MessageToDict(report) for report in response.task_reports ]
        else:
            return []

    def get_task_attempt_report(self, attempt_id, task_id=None):
        task_attempt = proto_utils.create_task_attempt_id_proto(attempt_id=attempt_id, task_id=task_id)
        response = self._getTaskAttemptReport(task_attempt_id=task_attempt)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    ''' Token Functions '''

    def get_delegation_token(self, renewer=None):
        response = self._getDelegationToken(renewer=renewer)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def renew_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._renewDelegationToken(token=token)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def cancel_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._cancelDelegationToken(token=token)
        return True

class YarnHistoryServerClient(YarnClient):
    """
      Yarn History Server applications client. Requires the timeline server to be setup.
    """

    #service_protocol = "org.apache.hadoop.yarn.api.ApplicationHistoryProtocolPB"
    service_protocol = "org.apache.hadoop.yarn.api.ApplicationHistoryProtocol"
    service_stub = application_history_client_protocol.ApplicationHistoryProtocolService_Stub

    _getApplicationReport = _RpcHandler( service_stub, service_protocol )
    _getApplications = _RpcHandler( service_stub, service_protocol )
    _getApplicationAttemptReport = _RpcHandler( service_stub, service_protocol )
    _getApplicationAttempts = _RpcHandler( service_stub, service_protocol )
    _getContainerReport = _RpcHandler( service_stub, service_protocol )
    _getContainers = _RpcHandler( service_stub, service_protocol )
    _getDelegationToken = _RpcHandler( service_stub, service_protocol )
    _renewDelegationToken = _RpcHandler( service_stub, service_protocol )
    _cancelDelegationToken = _RpcHandler( service_stub, service_protocol )

    ''' Application Functions '''

    def get_application_report(self, application_id, cluster_timestamp=None):
        application = yarn_protos.ApplicationIdProto(id=application_id, cluster_timestamp=cluster_timestamp)
        response = self._getApplicationReport(application)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_applications(self, application_types=None, application_states=None, users=None,
                         queues=None, limit=None, start_begin=None, start_end=None,
                         finish_begin=None, finish_end=None, applicationTags=None, scope=None):

        if application_types:
           if not type(application_types) in (tuple, list):
              application_types = [application_types]

        if users:
           if not type(users) in (tuple, list):
              users = [users]

        if queues:
           if not type(queues) in (tuple, list):
              queues = [queues]

        if start_begin:
           if not isinstance(start_begin, datetime):
              start_begin = int( (start_begin - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(start_begin, int):
              raise YarnError("only int and datetime are valid values for start_begin.")

        if start_end:
           if not isinstance(start_end, datetime):
              start_end = int( (start_end - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(start_end, int):
              raise YarnError("only int and datetime are valid values for start_end.")

        if finish_begin:
           if not isinstance(finish_begin, datetime):
              finish_begin = int( (finish_begin - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(finish_begin, int):
              raise YarnError("only int and datetime are valid values for finish_begin.")

        if finish_end:
           if not isinstance(finish_end, datetime):
              finish_end = int( (finish_end - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(finish_end, int):
              raise YarnError("only int and datetime are valid values for finish_end.")

        if applicationTags:
           if not type(applicationTags) in (tuple, list):
              applicationTags = [applicationTags]

        if application_states:
            if type(application_states) in (tuple, list):
                for app_state in application_states:
                   if not isinstance(app_state, proto_utils.APPLICATION_STATES):
                      raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")
            else:
                if isinstance(application_states, proto_utils.APPLICATION_STATES):
                    application_states = [application_states]
                else:
                    raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")

        if scope:
            if type(scope) in (tuple, list):
                for s in scope:
                    if not isinstance(s, proto_utils.APPLICATION_SCOPE):
                        raise YarnError("scope need to be a list of Enum APPLICATION_SCOPE.")
            else:
                if isinstance(scope, proto_utils.APPLICATION_SCOPE):
                    scope = [scope]
                else:
                    raise YarnError("scope need to be a list of Enum APPLICATION_SCOPE.")

        response = self._getApplications( application_types=application_types, application_states=application_states,
                                          users=users,queues=queues, limit=limit, start_begin=start_begin, start_end=start_end,
                                          finish_begin=finish_begin, finish_end=finish_end, applicationTags=applicationTags, scope=scope)
        if response:
            return [ json_format.MessageToDict(application) for application in response.applications ]
        else:
            return []

    ''' Application Attempts Functions '''

    def get_application_attempt_report(self, application_id=None, attemptId=None):
        application_attempt = proto_utils.create_application_attempt_id_proto(application_id=application_id, attemptId=attemptId)
        response = self._getApplicationAttemptReport(application_attempt_id=application_attempt)
        if response:
            return json_format.MessageToDict(response.application_attempt_report)
        else:
            return {}

    def get_application_attempts(self, application_id):
        if application_id:
            if not isinstance(application_id, yarn_protos.ApplicationIdProto):
                application_id = proto_utils.create_applicationid_proto(id=application_id)
        response = self._getApplicationAttempts(application_id=application_id)
        if response:
            return [ json_format.MessageToDict(attempt) for attempt in response.application_attempts ]
        else:
            return []

    ''' Container Functions '''

    def get_container_report(self, app_id, app_attempt_id, container_id):
        containerid = proto_utils.create_containerid_proto(app_id=app_id, app_attempt_id=app_attempt_id, container_id=container_id)
        response = self._getContainerReport(container_id=containerid)
        if response:
            return json_format.MessageToDict(response.container_report)
        else:
            return {}

    def get_containers(self, application_id=None, attemptId=None ):
        application_attempt = proto_utils.create_application_attempt_id_proto(application_id=application_id, attemptId=attemptId)
        response = self._getContainers(application_attempt_id=application_attempt)
        if response:
            return [ json_format.MessageToDict(container) for container in response.containers ]
        else:
            return []

    ''' Token Functions '''

    def get_delegation_token(self, renewer=None):
        response = self._getDelegationToken(renewer=renewer)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def renew_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._renewDelegationToken(token=token)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}  

    def cancel_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._cancelDelegationToken(token=token)
        return True  

class YarnRMApplicationClient(YarnFailoverClient):
    """
      Client<-->ResourceManager
      Implements the protocol between clients and the ResourceManager to submit/abort jobs
      and to get information on applications, cluster metrics, nodes, queues and ACLs. 
    """

    service_protocol = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
    service_stub = application_client_protocol.ApplicationClientProtocolService_Stub

    _getApplications = _RpcHandler( service_stub, service_protocol )
    _getClusterMetrics = _RpcHandler( service_stub, service_protocol )
    _getNewApplication = _RpcHandler( service_stub, service_protocol )
    _getApplicationReport = _RpcHandler( service_stub, service_protocol )
    _submitApplication = _RpcHandler( service_stub, service_protocol )
    _forceKillApplication = _RpcHandler( service_stub, service_protocol )
    _getClusterNodes = _RpcHandler( service_stub, service_protocol )
    _getQueueInfo = _RpcHandler( service_stub, service_protocol )
    _getQueueUserAcls = _RpcHandler( service_stub, service_protocol )
    _getDelegationToken = _RpcHandler( service_stub, service_protocol )
    _renewDelegationToken = _RpcHandler( service_stub, service_protocol )
    _cancelDelegationToken = _RpcHandler( service_stub, service_protocol )
    _moveApplicationAcrossQueues = _RpcHandler( service_stub, service_protocol )
    _getApplicationAttemptReport = _RpcHandler( service_stub, service_protocol )
    _getApplicationAttempts = _RpcHandler( service_stub, service_protocol )
    _getContainerReport = _RpcHandler( service_stub, service_protocol )
    _getContainers = _RpcHandler( service_stub, service_protocol )
    _getNodeToLabels = _RpcHandler( service_stub, service_protocol )
    _getClusterNodeLabels = _RpcHandler( service_stub, service_protocol )
    _submitReservation = _RpcHandler( service_stub, service_protocol )
    _updateReservation = _RpcHandler( service_stub, service_protocol )
    _deleteReservation = _RpcHandler( service_stub, service_protocol )


    def submit_application(self, application_id, application_name=None, queue =None,
                           priority=None, am_container_spec=None, cancel_tokens_when_complete=True,
                           unmanaged_am=False, maxAppAttempts=0, resource=None, applicationType="YARN",
                           keep_containers_across_application_attempts=False, applicationTags=None,
                           attempt_failures_validity_interval=1, log_aggregation_context=None,
                           reservation_id=None, node_label_expression=None, am_container_resource_request=None):
        '''
          The interface used by clients to submit a new application to the ResourceManager.
          The client is required to provide details such as queue, Resource required to run the ApplicationMaster, 
          the equivalent of ContainerLaunchContext for launching the ApplicationMaster etc. via the SubmitApplicationRequest.

          Currently the ResourceManager sends an immediate (empty) SubmitApplicationResponse on accepting the submission and throws 
          an exception if it rejects the submission. However, this call needs to be followed by getApplicationReport(GetApplicationReportRequest)
          to make sure that the application gets properly submitted - obtaining a SubmitApplicationResponse from ResourceManager doesn't guarantee
          that RM 'remembers' this application beyond failover or restart. If RM failover or RM restart happens before ResourceManager saves the
          application's state successfully, the subsequent getApplicationReport(GetApplicationReportRequest) will throw a ApplicationNotFoundException.
          The Clients need to re-submit the application with the same ApplicationSubmissionContext when it encounters the ApplicationNotFoundException
          on the getApplicationReport(GetApplicationReportRequest) call.

          During the submission process, it checks whether the application already exists. If the application exists, it will simply return
          SubmitApplicationResponse.

          In secure mode,the <code>ResourceManager</code> verifies access to queues etc. before accepting the application submission.

          @param request request to submit a new application
          @return (empty) response on accepting the submission
        '''

        if priority:
            if not isinstance(priority, yarn_protos.PriorityProto):
                priority = proto_utils.create_priority_proto(priority=priority)

        if application_id:
            if not isinstance(application_id, yarn_protos.ApplicationIdProto):
                application_id = proto_utils.create_applicationid_proto(id=application_id)

        if am_container_spec:
            if not isinstance(am_container_spec, yarn_protos.ContainerLaunchContextProto):
                raise YarnError("am_container_spec need to be of type ContainerLaunchContextProto.")

        if resource:
            if not isinstance(resource, yarn_protos.ResourceProto):
                raise YarnError("resource need to be of type ResourceProto.")

        if log_aggregation_context:
            if not isinstance(log_aggregation_context, yarn_protos.LogAggregationContextProto):
                raise YarnError("log_aggregation_context need to be of type LogAggregationContextProto.")

        if reservation_id:
            if not isinstance(reservation_id, yarn_protos.ReservationIdProto):
                reservation_id = proto_utils.create_reservationid_proto(id=application_id)

        if am_container_resource_request:
            if not isinstance(am_container_resource_request, yarn_protos.ResourceRequestProto):
                raise YarnError("am_container_resource_request need to be of type ResourceRequestProto.")

        submission_context = yarn_protos.ApplicationSubmissionContextProto( application_id=application_id, application_name=application_name, queue=queue,
                                                                            priority=priority, am_container_spec=am_container_spec,
                                                                            cancel_tokens_when_complete=cancel_tokens_when_complete,
                                                                            unmanaged_am=unmanaged_am, maxAppAttempts=maxAppAttempts, resource=resource,
                                                                            applicationType=applicationType,
                                                                            keep_containers_across_application_attempts=keep_containers_across_application_attempts,
                                                                            applicationTags=applicationTags,
                                                                            attempt_failures_validity_interval=attempt_failures_validity_interval,
                                                                            log_aggregation_context=log_aggregation_context, reservation_id=reservation_id,
                                                                            node_label_expression=node_label_expression,
                                                                            am_container_resource_request=am_container_resource_request)
        response = self._submitApplication(application_submission_context=submission_context)
        return True

    def get_delegation_token(self, renewer=None):
        response = self._getDelegationToken(renewer=renewer)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def renew_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._renewDelegationToken(token=token)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}  

    def cancel_delegation_token(self, token):
        if not isinstance(token, security_protocol.TokenProto):
            raise YarnError("token need to be of type TokenProto.")
        response = self._cancelDelegationToken(token=token)
        return True  

    def move_application_across_queues(self, application_id, target_queue):
        '''
           Move an application to a new queue. 
           @param application_id: the application ID
           @param target_queue: the target queue
           @return an empty response
        '''

        if not isinstance(application_id, yarn_protos.ApplicationIdProto):
            application_id = proto_utils.create_applicationid_proto(id=application_id)
        response = self._moveApplicationAcrossQueues(application_id=application_id, target_queue=target_queue)
        return True

    def get_application_attempt_report(self, application_id=None, attemptId=None):
        application_attempt = proto_utils.create_application_attempt_id_proto(application_id=application_id, attemptId=attemptId)
        response = self._getApplicationAttemptReport(application_attempt_id=application_attempt)
        if response:
            return json_format.MessageToDict(response.application_attempt_report)
        else:
            return {}

    def get_application_attempts(self, application_id):
        if application_id:
            if not isinstance(application_id, yarn_protos.ApplicationIdProto):
                application_id = proto_utils.create_applicationid_proto(id=application_id)
        response = self._getApplicationAttempts(application_id=application_id)
        if response:
            return [ json_format.MessageToDict(attempt) for attempt in response.application_attempts ]
        else:
            return []

    def get_container_report(self, app_id, app_attempt_id, container_id):
        containerid = proto_utils.create_containerid_proto(app_id=app_id, app_attempt_id=app_attempt_id, container_id=container_id)
        response = self._getContainerReport(container_id=containerid)
        if response:
            return json_format.MessageToDict(response.container_report)
        else:
            return {}

    def get_containers(self, application_id=None, attemptId=None ):
        application_attempt = proto_utils.create_application_attempt_id_proto(application_id=application_id, attemptId=attemptId)
        response = self._getContainers(application_attempt_id=application_attempt)
        if response:
            return [ json_format.MessageToDict(container) for container in response.containers ]
        else:
            return []

    def submit_reservation(self, reservation_resources=None, arrival=None, deadline=None, reservation_name=None, queue=None, interpreter=None):
        '''
          The interface used by clients to submit a new reservation to the ResourceManager.

          The client packages all details of its request in a ReservationSubmissionRequest object.
          This contains information about the amount of capacity, temporal constraints, and concurrency needs.
          Furthermore, the reservation might be composed of multiple stages, with ordering dependencies among them.

          In order to respond, a new admission control component in the ResourceManager performs an analysis of
          the resources that have been committed over the period of time the user is requesting, verify that
          the user requests can be fulfilled, and that it respect a sharing policy (e.g., CapacityOverTimePolicy).
          Once it has positively determined that the ReservationSubmissionRequest is satisfiable the
          ResourceManager answers with a ReservationSubmissionResponse that include a non-null ReservationId.

          Upon failure to find a valid allocation the response is an exception with the reason.
          On application submission the client can use this ReservationId to obtain access to the reserved resources.

          The system guarantees that during the time-range specified by the user, the reservationID will be corresponding
          to a valid reservation. The amount of capacity dedicated to such queue can vary overtime, depending of the
          allocation that has been determined. But it is guaranteed to satisfy all the constraint expressed by the user in the
          ReservationSubmissionRequest.

          @param request the request to submit a new Reservation
          @return response the ReservationId on accepting the submission
        '''
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
            if not isinstance(interpreter, proto_utils.RESERVATION_REQUEST_INTERPRETER):
                raise YarnError("interpreter need to be of type Enum RESERVATION_REQUEST_INTERPRETER.")
        
        reservation_requests = yarn_protos.ReservationRequestsProto(reservation_resources=reservation_resources, interpreter=interpreter)
        reservation_definition = yarn_protos.ReservationDefinitionProto(reservation_requests=reservation_requests, arrival=arrival, deadline=deadline, reservation_name=reservation_name)
        
        response = self._submitReservation(queue=queue, reservation_definition=reservation_definition)

        if response:
            return json_format.MessageToDict(response.reservation_id)
        else:
            return {}

    def update_reservation(self, reservation_id, reservation_resources=None, arrival=None, deadline=None, reservation_name=None, interpreter=None):
        if reservation_id:
            if not isinstance(reservation_id, yarn_protos.ReservationIdProto):
                reservation_id = proto_utils.create_reservationid_proto(id=application_id)

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
            if not isinstance(interpreter, proto_utils.RESERVATION_REQUEST_INTERPRETER):
                raise YarnError("interpreter need to be of type Enum RESERVATION_REQUEST_INTERPRETER.")
        
        reservation_requests = yarn_protos.ReservationRequestsProto(reservation_resources=reservation_resources, interpreter=interpreter)
        reservation_definition = yarn_protos.ReservationDefinitionProto(reservation_requests=reservation_requests, arrival=arrival, deadline=deadline, reservation_name=reservation_name)
        response = self._submitReservation(queue=queue, reservation_definition=reservation_definition)
        return True

    def delete_reservation(self, reservation_id):
        if reservation_id:
            if not isinstance(reservation_id, yarn_protos.ReservationIdProto):
                reservation_id = proto_utils.create_reservationid_proto(id=application_id)
                
        response = self._deleteReservation(reservation_id=reservation_id)
        return True

    def force_kill_application(self, application_id):
        if application_id:
            if not isinstance(application_id, yarn_protos.ApplicationIdProto):
                application_id = proto_utils.create_applicationid_proto(id=application_id)
        response = self._forceKillApplication(application_id=application_id)
        return True

    def get_application_report(self, application_id, cluster_timestamp=None):
        application = yarn_protos.ApplicationIdProto(id=application_id, cluster_timestamp=cluster_timestamp)
        response = self._getApplicationReport(application)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_new_application(self):
        '''
          The interface used by clients to obtain a new ApplicationId for submitting new applications.
          The ResourceManager responds with a new, monotonically increasing, ApplicationId which is used 
          by the client to submit a new application.
          The ResourceManager also responds with details such  as maximum resource capabilities in the cluster
          as specified in GetNewApplicationResponse.

          @return: response containing the new <code>ApplicationId</code> to be used to submit an application.
        '''
        response = self._getNewApplication()
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_cluster_metrics(self):
        response = self._getClusterMetrics()
        if response:
            return json_format.MessageToDict(response.cluster_metrics)
        else:
            return {}

    def get_cluster_nodes(self, node_states=None):
        if node_states:
            if type(node_states) in (tuple, list):
                for n_state in node_states:
                   if not isinstance(n_state, proto_utils.NODE_STATES):
                      raise YarnError("node_states need to be a list of Enum NODE_STATES.")
            else:
                if isinstance(node_states, proto_utils.NODE_STATES):
                    node_states = [node_states]
                else:
                    raise YarnError("node_states need to be a list of Enum NODE_STATES.")

        response = self._getClusterNodes(nodeStates=node_states)
        if response:
            return json_format.MessageToDict(response.nodeReports)
        else:
            return {}

    def get_node_to_labels(self):
        response = self._getNodeToLabels()
        if response:
            return [ json_format.MessageToDict(label) for label in response.nodeToLabels ]
        else:
            return []

    def get_cluster_node_labels(self):
        response = self._getClusterNodeLabels()
        if response:
            return [ json_format.MessageToDict(label) for label in response.nodeLabels ]
        else:
            return []

    def get_queue_info(self, queue_name, include_applications=False, include_child_queues=False, recursive=False):
        response = self._getQueueInfo( queueName=queue_name, includeApplications=include_applications,
                                       includeChildQueues=include_child_queues, recursive=recursive)
        if response:
            return json_format.MessageToDict(response.queueInfo)
        else:
            return {}

    def get_queue_user_acls(self):
        response = self._getQueueUserAcls()
        if response:
            return json_format.MessageToDict(response.queueUserAcls)
        else:
            return {}

    def get_applications(self, application_types=None, application_states=None, users=None,
                         queues=None, limit=None, start_begin=None, start_end=None,
                         finish_begin=None, finish_end=None, applicationTags=None, scope=None):

        if application_types:
           if not type(application_types) in (tuple, list):
              application_types = [application_types]

        if users:
           if not type(users) in (tuple, list):
              users = [users]

        if queues:
           if not type(queues) in (tuple, list):
              queues = [queues]

        if start_begin:
           if not isinstance(start_begin, datetime):
              start_begin = int( (start_begin - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(start_begin, int):
              raise YarnError("only int and datetime are valid values for start_begin.")

        if start_end:
           if not isinstance(start_end, datetime):
              start_end = int( (start_end - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(start_end, int):
              raise YarnError("only int and datetime are valid values for start_end.")

        if finish_begin:
           if not isinstance(finish_begin, datetime):
              finish_begin = int( (finish_begin - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(finish_begin, int):
              raise YarnError("only int and datetime are valid values for finish_begin.")

        if finish_end:
           if not isinstance(finish_end, datetime):
              finish_end = int( (finish_end - datetime.utcfromtimestamp(0)).total_seconds() * 1000.0)
           elif not isinstance(finish_end, int):
              raise YarnError("only int and datetime are valid values for finish_end.")

        if applicationTags:
           if not type(applicationTags) in (tuple, list):
              applicationTags = [applicationTags]

        if application_states:
            if type(application_states) in (tuple, list):
                for app_state in application_states:
                   if not isinstance(app_state, proto_utils.APPLICATION_STATES):
                      raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")
            else:
                if isinstance(application_states, proto_utils.APPLICATION_STATES):
                    application_states = [application_states]
                else:
                    raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")

        if scope:
            if type(scope) in (tuple, list):
                for s in scope:
                    if not isinstance(s, proto_utils.APPLICATION_SCOPE):
                        raise YarnError("scope need to be a list of Enum APPLICATION_SCOPE.")
            else:
                if isinstance(scope, proto_utils.APPLICATION_SCOPE):
                    scope = [scope]
                else:
                    raise YarnError("scope need to be a list of Enum APPLICATION_SCOPE.")

        response = self._getApplications( application_types=application_types, application_states=application_states,
                                          users=users,queues=queues, limit=limit, start_begin=start_begin, start_end=start_end,
                                          finish_begin=finish_begin, finish_end=finish_end, applicationTags=applicationTags, scope=scope)
        if response:
            return [ json_format.MessageToDict(application) for application in response.applications ]
        else:
            return []

class YarnApplicationMasterClient(YarnClient):
    """
      ApplicationMaster<-->ResourceManager
      Yarn Application Master Resource Manager client. Implements the protocol between a live instance of ApplicationMaster
      and the Resource Manager.
      This is used by the ApplicationMaster to register/unregister and to request and obtain recources in the cluster from
      the resource manager
      RPC interface needed by the application Master
      to request resources from the resource manager.
    """

    service_protocol = "org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB"
    service_stub = applicationmaster_protocol.ApplicationMasterProtocolService_Stub

    _registerApplicationMaster = _RpcHandler( service_stub, service_protocol )
    _finishApplicationMaster = _RpcHandler( service_stub, service_protocol )
    _allocate = _RpcHandler( service_stub, service_protocol )

    def register_application_master(self, host=None, rpc_port=None, tracking_url=None):
        '''
            The application master register itself with the resource manager once started.
        '''
        response = self._registerApplicationMaster(host=host, rpc_port=rpc_port, tracking_url=tracking_url)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def finish_application_master(self, diagnostics, tracking_url, final_application_status):
        '''
            The application master finish its execution.
        '''
        if final_application_status:
            if not isinstance(final_application_status, proto_utils.FINAL_APPLICATION_STATUS):
                raise YarnError("final_application_status need to be of type FINAL_APPLICATION_STATUS.")
        response = self._finishApplicationMaster(diagnostics=diagnostics, tracking_url=tracking_url, final_application_status=final_application_status)
        if response:
            return json_format.MessageToDict(response.isUnregistered)
        else:
            return False

    def allocate(self, ask, release, blacklist_request, response_id, progress, increase_request):
        if ask:
            if not isinstance(ask, yarn_protos.ResourceRequestProto):
                raise YarnError("ask need to be of type ResourceRequestProto.")
        if release:
            if not isinstance(release, yarn_protos.ContainerIdProto):
                raise YarnError("release need to be of type ContainerIdProto.")
        if blacklist_request:
            if not isinstance(blacklist_request, yarn_protos.ResourceBlacklistRequestProto):
                raise YarnError("blacklist_request need to be of type ResourceBlacklistRequestProto.")
        if increase_request:
            if not isinstance(increase_request, yarn_protos.ContainerResourceIncreaseRequestProto):
                raise YarnError("increase_request need to be of type ContainerResourceIncreaseRequestProto.")
        response = self._allocate(ask=ask, release=release, blacklist_request=blacklist_request, response_id=response_id, progress=progress, increase_request=increase_request)
        if response:
            return json_format.MessageToDict(response)
        else:
            return False

class YarnContainerManagerClient(YarnClient):
    """
     ApplicationMaster<-->NodeManager
     Yarn Containers Management Client, implements the protocol between an ApplicationMaster and a NodeManager to 
     start/stop and increase resource of containers and to get status of running containers.

     In Yarn the ResourceManager hands off control of assigned NodeManagers to the ApplicationMaster.
     The ApplicationMaster independently contact its assigned node managers and provide them with a Container Launch Context 
     that includes environment variables, dependencies located in remote storage, security tokens, and commands needed to start the actual process.

     If security is enabled the NodeManager verifies that the ApplicationMaster has truly been allocated the container
     by the ResourceManager and also verifies all interactions such as stopping the container or obtaining status information for the container.
    """

    service_protocol = "org.apache.hadoop.yarn.api.ContainerManagementProtocolPB"
    service_stub = container_management_protocol.ContainerManagementProtocolService_Stub

    _startContainers = _RpcHandler( service_stub, service_protocol )
    _stopContainers = _RpcHandler( service_stub, service_protocol )
    _getContainerStatuses = _RpcHandler( service_stub, service_protocol )

    def start_containers(self, start_container_request):
        if start_container_request:
            if type(start_container_request) in (tuple, list):
                for request in start_container_request:
                   if not isinstance(request, yarn_service_protos.StartContainerRequestProto):
                      raise YarnError("start_container_request need to be a list of Type StartContainerRequestProto.")
            else:
                if isinstance(start_container_request, yarn_service_protos.StartContainerRequestProto):
                    start_container_request = [start_container_request]
                else:
                    raise YarnError("start_container_request need to be a list of Type StartContainerRequestProto.")

        response = self._startContainers(start_container_request=start_container_request)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def stop_containers(self,container_id):
        if container_id:
            if type(container_id) in (tuple, list):
                for container in container_id:
                   if not isinstance(container, yarn_protos.ContainerIdProto):
                      raise YarnError("container_id need to be a list of Type ContainerIdProto.")
            else:
                if isinstance(container_id, yarn_protos.ContainerIdProto):
                    container_id = [container_id]
                else:
                    raise YarnError("container_id need to be a list of Type ContainerIdProto.")
    
        response = self._stopContainers(container_id=container_id)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}

    def get_container_statuses(self,container_id):
        if container_id:
            if type(container_id) in (tuple, list):
                for container in container_id:
                   if not isinstance(container, yarn_protos.ContainerIdProto):
                      raise YarnError("container_id need to be a list of Type ContainerIdProto.")
            else:
                if isinstance(container_id, yarn_protos.ContainerIdProto):
                    container_id = [container_id]
                else:
                    raise YarnError("container_id need to be a list of Type ContainerIdProto.")
    
        response = self._getContainerStatuses(container_id=container_id)
        if response:
            return json_format.MessageToDict(response)
        else:
            return {}
