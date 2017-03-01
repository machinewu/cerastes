#!/usr/bin/python

import pyarn.protobuf.resourcemanager_administration_protocol_pb2 as rm_protocol
import pyarn.protobuf.yarn_server_resourcemanager_service_protos_pb2 as yarn_rm_service_protos

from pyarn.errors import YarnError
from pyarn.service import RpcService
from . import DEFAULT_YARN_PROTOCOL_VERSION

import logging as lg

log = lg.getLogger(__name__)

class RMClient(object):
    """
    A pure python Yarn administration client.
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
        #if version < 9:
        #    raise Exception("Only protocol versions >= 9 supported")

        context_proto = "org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB"
        self.host = host
        self.port = port
        self.use_sasl = use_sasl
        self.yarn_rm_principal = yarn_rm_principal
        self.service_stub_class = rm_protocol.ResourceManagerAdministrationProtocolService_Stub

        self.service = RpcService(self.service_stub_class, self.port, self.host, version,
                                  context_proto, effective_user,self.use_sasl,
                                  self.yarn_rm_principal, sock_connect_timeout, sock_request_timeout)

        log.debug("Created client for %s:%s", host, port)

    def get_groups_for_user(self, user):
        req = yarn_rm_service_protos.GetGroupsForUserRequestProto()
        req.user = user
        response = self.service.getGroupsForUser(req)
        if response:
            return [ group for group in response.groups ]
        else:
            return []
