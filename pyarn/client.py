#!/usr/bin/python

import pyarn.protobuf.resourcemanager_administration_protocol_pb2 as rm_protocol
import pyarn.protobuf.yarn_server_resourcemanager_service_protos_pb2 as yarn_rm_service_protos
import pyarn.protobuf.applicationclient_protocol_pb2 as application_client_protocol
import pyarn.protobuf.yarn_service_protos_pb2 as yarn_service_protos
import pyarn.protobuf.yarn_protos_pb2 as yarn_protos

from pyarn.errors import RpcError, YarnError, AuthorizationException, StandbyError 
from pyarn.controller import SocketRpcController
from pyarn.channel import SocketRpcChannel
from pyarn.utils import SyncServicesList

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
     Abstract Yarn Client.
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
                                        context_protocol=self.get_protocol(), effective_user=effective_user,
                                        use_sasl=use_sasl, krb_principal=yarn_rm_principal,
                                        sock_connect_timeout=sock_connect_timeout,
                                        sock_request_timeout=sock_request_timeout)

        self.service = self.get_service_stub()(self.channel)

        log.debug("Created client for %s:%s", host, port)

    @abstractmethod
    def get_protocol(self):
        return

    @abstractmethod
    def get_service_stub(self):
        return
   
    def _call(self, executor, controller, request):
        response =  executor(self.service, controller, request)
        return response

class YarnHAClient(YarnClient):
    """
     Abstract Yarn HA Client. Extend this class if the service is conserned by HA.
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
                                   context_protocol=self.get_protocol(), effective_user=self.effective_user,
                                   use_sasl=self.use_sasl, krb_principal=self.yarn_rm_principal,
                                   sock_connect_timeout=self.sock_connect_timeout,
                                   sock_request_timeout=self.sock_request_timeout)

        return self.get_service_stub()(channel)
    
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

class YarnRMAdminClient(YarnHAClient):
    """
      Yarn Resource Manager administration client.
      Typically on port 8033.
    """

    rm_admin_proto = "org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB"
    rm_admin_service_stub = rm_protocol.ResourceManagerAdministrationProtocolService_Stub

    _getGroupsForUser = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshServiceAcls = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshAdminAcls = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshNodes = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshQueues = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshSuperUserGroupsConfiguration = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshUserToGroupsMappings = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _updateNodeResource = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _addToClusterNodeLabels = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _removeFromClusterNodeLabels = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _replaceLabelsOnNodes = _RpcHandler( rm_admin_service_stub, rm_admin_proto )

    def get_protocol(self):
        return self.rm_admin_proto

    def get_service_stub(self):
        return self.rm_admin_service_stub

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

class YarnRMApplicationClient(YarnHAClient):
    """
      Yarn Resource Manager applications client.
      Typically on port 8032.
    """

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

    rm_app_proto = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
    rm_app_service_stub = application_client_protocol.ApplicationClientProtocolService_Stub

    _getApplications = _RpcHandler( rm_app_service_stub, rm_app_proto )

    def get_protocol(self):
        return self.rm_app_proto

    def get_service_stub(self):
        return self.rm_app_service_stub

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
                   if not isinstance(app_state, self.APPLICATION_STATES):
                      raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")
            else:
                if isinstance(application_states, self.APPLICATION_STATES):
                    application_states = [application_states]
                else:
                    raise YarnError("application_states need to be a list of Enum APPLICATION_STATES.")

        if scope:
            if type(scope) in (tuple, list):
                for s in scope:
                    if not isinstance(s, self.APPLICATION_SCOPE):
                        raise YarnError("scope need to be a list of Enum APPLICATION_SCOPE.")
            else:
                if isinstance(scope, self.APPLICATION_SCOPE):
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
