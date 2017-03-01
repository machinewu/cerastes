#!/usr/bin/python
# Copyright (c) 2009 Las Cumbres Observatory (www.lcogt.net)
# Copyright (c) 2010 Jan Dittberner
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from pyarn.channel import SocketRpcChannel
import google.protobuf.service as service


class RpcService(object):
    def __init__(self, service_stub_class, port, host, hadoop_version, context_protocol, effective_user=None,
                 use_sasl=False, yarn_rm_principal=None, sock_connect_timeout=10000,
                 sock_request_timeout=10000):
        self.service_stub_class = service_stub_class
        self.port = port
        self.host = host

        # Setup the RPC channel
        self.channel = SocketRpcChannel(host=self.host, port=self.port, version=hadoop_version,
                                        context_protocol=context_protocol, effective_user=effective_user,
                                        use_sasl=use_sasl, yarn_rm_principal=yarn_rm_principal,
                                        sock_connect_timeout=sock_connect_timeout,
                                        sock_request_timeout=sock_request_timeout,)
        self.service = self.service_stub_class(self.channel)

        # go through service_stub methods and add a wrapper function to
        # this object that will call the method
        for method in service_stub_class.GetDescriptor().methods:
            # Add service methods to the this object
            rpc = lambda request, service=self, method=method.name: service.call(service_stub_class.__dict__[method], request)

            self.__dict__[method.name] = rpc

    def call(self, method, request):
        controller = SocketRpcController()
        return method(self.service, controller, request)


class SocketRpcController(service.RpcController):
    ''' RpcController implementation to be used by the SocketRpcChannel class.
    The RpcController is used to mediate a single method call.
    '''

    def __init__(self):
        '''Constructor which initializes the controller's state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def handleError(self, error_code, message):
        '''Log and set the controller state.'''
        self._fail = True
        self.reason = error_code
        self._error = message

    def reset(self):
        '''Resets the controller i.e. clears the error state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def failed(self):
        '''Returns True if the controller is in a failed state.'''
        return self._fail

    def error(self):
        return self._error
