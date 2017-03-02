#!/usr/bin/python

import pyarn.protobuf.resourcemanager_administration_protocol_pb2 as rm_protocol
import pyarn.protobuf.yarn_server_resourcemanager_service_protos_pb2 as yarn_rm_service_protos

from pyarn.errors import RpcError, YarnError, AuthorizationException, StandbyError 
from pyarn.controller import SocketRpcController
from pyarn.channel import SocketRpcChannel

from google.protobuf import reflection
from six import add_metaclass
from abc import ABCMeta, abstractmethod

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
        request = req_class()
        print params
        for key in params:
            try:
                setattr(request, key, params[key])
            except AttributeError as ex:
                raise YarnError("Assignment not allowed : no field %s in protocol message %s" % (key, method))
        try:
            response = client._call(rpc_executor, controller, request)
            return response
        # parse the error message and return the corresponding exception
        except RpcError as e:
            if "AuthorizationException" in str(e):
                raise AuthorizationException("user not authorized to perform operation : %s" % str(e))
            elif "StandbyException" in str(e):
                raise StandbyError("resource manager host %s in standby mode : %s." % (client.host, str(e)))
            else:
                raise YarnError("Error running yarn method: %s" % str(e)) 
    
    return rpc_handler

# Thanks Matt for the nice piece of code
# inherit ABCMeta to make YarnClient abstract
class _ClientType(ABCMeta):
  pattern = re.compile(r'_|\d')
  def __new__(mcs, name, bases, attrs):
    for key, value in attrs.items():
      if isinstance(value, _RpcHandler):
        attrs[key] = value.get_handler(mcs.pattern.sub('', key))
        #print "%s , %s -> %s" % (key, attrs[key],mcs.pattern.sub('', key).upper())
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

class YarnRMAdminClient(YarnClient):
    """
      Yarn Resource Manager administration client.
    """

    rm_admin_proto = "org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB"
    rm_admin_service_stub = rm_protocol.ResourceManagerAdministrationProtocolService_Stub

    _getGroupsForUser = _RpcHandler( rm_admin_service_stub, rm_admin_proto )
    _refreshServiceAcls = _RpcHandler( rm_admin_service_stub, rm_admin_proto )

    def get_protocol(self):
        return self.rm_admin_proto

    def get_service_stub(self):
        return self.rm_admin_service_stub

    def refresh_service_acls(self):
        response = self._refreshServiceAcls()
        return True

    def get_groups_for_user(self, user):
        response = self._getGroupsForUser(user=user)
        if response:
            return [ group for group in response.groups ]
        else:
            return []
