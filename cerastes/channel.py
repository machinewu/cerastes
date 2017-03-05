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

'''
channel.py - Socket implementation of Google's Protocol Buffers RPC service interface.

This package contains classes providing a socket implementation of the RPCChannel abstract class.
Original Authors: Martin Norbury (mnorbury@lcogt.net)
         Eric Saunders (esaunders@lcogt.net)
         Jan Dittberner (jan@dittberner.info)
Nov 2010 Wouter de Bie (wouter@spotify.com)
Feb 2017 Yassine Azzouz (yassine.azzouz@gmail.com)
'''

# Standard library imports
import socket
import os
import math
import pwd

# Third party imports
from google.protobuf.service import RpcChannel
import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

# Protobuf imports
from cerastes.protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from cerastes.protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto
from cerastes.protobuf.ProtobufRpcEngine_pb2 import RequestHeaderProto

from cerastes.errors import RpcError, RpcAuthenticationError, MalformedRpcRequestError, RpcSaslError, RpcBufferError 

# Module imports

import logging as lg
import logging
import struct
import uuid

_kerberos_available = False
try:
    from cerastes.rpc_sasl import SaslRpcClient
    from cerastes.kerberos import Kerberos
    from krbV import Krb5Error
except ImportError:
    _kerberos_available = False
else:
    _kerberos_available = True

# Configure package logging
log = lg.getLogger(__name__)


def format_bytes(bytes):
    ascii = binascii.b2a_hex(bytes)
    byte_array = [ascii[i:i + 2] for i in range(0, len(ascii), 2)]
    return  "%s (len: %d)"% (" ".join(byte_array), len(byte_array))

def log_protobuf_message(header, message):
    log.debug("%s:\n\n\033[92m%s\033[0m" % (header, message))


def get_delimited_message_bytes(byte_stream, nr=4):
    ''' Parse a delimited protobuf message. This is done by first getting a protobuf varint from
    the stream that represents the length of the message, then reading that amount of
    from the message and then parse it.
    Since the int can be represented as max 4 bytes, first get 4 bytes and try to decode.
    The decoder returns the value and the position where the value was found, so we need
    to rewind the buffer to the position, because the remaining bytes belong to the message
    after.
    '''

    (length, pos) = decoder._DecodeVarint32(byte_stream.read(nr), 0)
    if log.getEffectiveLevel() == logging.DEBUG:
        log.debug("Delimited message length (pos %d): %d" % (pos, length))

    delimiter_bytes = nr - pos

    byte_stream.rewind(delimiter_bytes)
    message_bytes = byte_stream.read(length)
    if log.getEffectiveLevel() == logging.DEBUG:
        log.debug("Delimited message bytes (%d): %s" % (len(message_bytes), format_bytes(message_bytes)))

    total_len = length + pos
    return (total_len, message_bytes)


class RpcBufferedReader(object):
    '''Class that wraps a socket and provides some utility methods for reading
    and rewinding of the buffer. This comes in handy when reading protobuf varints.
    '''
    MAX_READ_ATTEMPTS = 100

    def __init__(self, socket):
        self.socket = socket
        self.reset()

    def read(self, n):
        '''Reads n bytes into the internal buffer'''
        bytes_wanted = n - self.buffer_length + self.pos + 1
        if bytes_wanted > 0:
            self._buffer_bytes(bytes_wanted)

        end_pos = self.pos + n
        ret = self.buffer[self.pos + 1:end_pos + 1]
        self.pos = end_pos
        return ret

    def _buffer_bytes(self, n):
        to_read = n
        for _ in xrange(self.MAX_READ_ATTEMPTS):
            bytes_read = self.socket.recv(to_read)
            self.buffer += bytes_read
            to_read -= len(bytes_read)
            if to_read == 0:
                log.debug("Bytes read: %d, total: %d" % (len(bytes_read), self.buffer_length))
                return n
        if len(bytes_read) < n:
            # we'd like to distinguish transient (e.g. network-related) problems
            # note: but this error could also be a logic error
            raise RpcBufferError("RpcBufferedReader only managed to read %s out of %s bytes" % (len(bytes_read), n))

    def rewind(self, places):
        '''Rewinds the current buffer to a position. Needed for reading varints,
        because we might read bytes that belong to the stream after the varint.
        '''
        log.debug("Rewinding pos %d with %d places" % (self.pos, places))
        self.pos -= places
        log.debug("Reset buffer to pos %d" % self.pos)

    def reset(self):
        self.buffer = ""
        self.pos = -1  # position of last byte read

    @property
    def buffer_length(self):
        '''Returns the length of the current buffer.'''
        return len(self.buffer)


class SocketRpcChannel(RpcChannel):
    ERROR_BYTES = 18446744073709551615L
    RPC_HEADER = "hrpc"
    RPC_SERVICE_CLASS = 0x00
    AUTH_PROTOCOL_NONE = 0x00
    AUTH_PROTOCOL_SASL = 0xDF
    RPC_PROTOCOL_BUFFFER = 0x02


    '''Socket implementation of an RpcChannel.
    '''
    def __init__(self, host, port, version, context_protocol, effective_user=None, use_sasl=False, krb_principal=None,
                 sock_connect_timeout=10000, sock_request_timeout=10000):
        '''SocketRpcChannel to connect to a socket server on a user defined port.
           It possible to define version and effective user for the communication.'''
        self.host = host
        self.port = port
        self.sock = None
        self.call_id = -3  # First time (when the connection context is sent, the call_id should be -3, otherwise start with 0 and increment)
        self.version = version
        self.client_id = str(uuid.uuid4())
        self.use_sasl = use_sasl
        self.krb_principal = krb_principal
        self.context_protocol=context_protocol
        if self.use_sasl:
            if not _kerberos_available:
                raise RpcError("Kerberos libs not found: pip install python-krbV sasl.")

            kerberos = Kerberos()
            try:
                if effective_user is not None:
                    self.effective_user = effective_user
                else:
                    self.effective_user = kerberos.user_principal().name
            except Krb5Error as ex:
                raise RpcAuthenticationError("Failed kerberos authentication : %s" % str(ex)) 
        else: 
            self.effective_user = effective_user or pwd.getpwuid(os.getuid())[0]
        self.sock_connect_timeout = sock_connect_timeout
        self.sock_request_timeout = sock_request_timeout

    def validate_request(self, request):
        '''Validate the client request against the protocol file.'''

        # Check the request is correctly initialized
        if not request.IsInitialized():
            raise MalformedRpcRequestError("Client request (%s) is missing mandatory fields" % type(request))

    def get_connection(self, host, port):
        '''Open a socket connection to a given host and port and writes the Hadoop header
        The Hadoop RPC protocol looks like this when creating a connection:
        +---------------------------------------------------------------------+
        |  Header, 4 bytes ("hrpc")                                           |
        +---------------------------------------------------------------------+
        |  Version, 1 byte (default verion 9)                                 |
        +---------------------------------------------------------------------+
        |  RPC service class, 1 byte (0x00)                                   |
        +---------------------------------------------------------------------+
        |  Auth protocol, 1 byte (Auth method None = 0)                       |
        +---------------------------------------------------------------------+
        |  Length of the RpcRequestHeaderProto  + length of the               |
        |  of the IpcConnectionContextProto (4 bytes/32 bit int)              |
        +---------------------------------------------------------------------+
        |  Serialized delimited RpcRequestHeaderProto                         |
        +---------------------------------------------------------------------+
        |  Serialized delimited IpcConnectionContextProto                     |
        +---------------------------------------------------------------------+
        '''

        log.debug("############## CONNECTING ##############")
        # Open socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(self.sock_connect_timeout / 1000)
        # Connect socket to server - defined by host and port arguments
        self.sock.connect((host, port))
        self.sock.settimeout(self.sock_request_timeout / 1000)

        # Send RPC headers
        self.write(self.RPC_HEADER)                             # header
        self.write(struct.pack('B', self.version))              # version
        self.write(struct.pack('B', self.RPC_SERVICE_CLASS))    # RPC service class
        if self.use_sasl:
            self.write(struct.pack('B', self.AUTH_PROTOCOL_SASL))   # serialization type (protobuf = 0xDF)
        else:
            self.write(struct.pack('B', self.AUTH_PROTOCOL_NONE))   # serialization type (protobuf = 0)

        if self.use_sasl:
            sasl = SaslRpcClient(self, krb_principal=self.krb_principal)
            sasl_connected = sasl.connect()
            if not sasl_connected:
                raise RpcSaslError("SASL is configured, but cannot get connected")

        rpc_header = self.create_rpc_request_header()
        context = self.create_connection_context()

        header_length = len(rpc_header) + encoder._VarintSize(len(rpc_header)) +len(context) + encoder._VarintSize(len(context))

        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("Header length: %s (%s)" % (header_length, format_bytes(struct.pack('!I', header_length))))

        self.write(struct.pack('!I', header_length))

        self.write_delimited(rpc_header)
        self.write_delimited(context)
    
    def write(self, data):
        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("Sending: %s", format_bytes(data))
        self.sock.send(data)

    def write_delimited(self, data):
        self.write(encoder._VarintBytes(len(data)))
        self.write(data)

    def create_rpc_request_header(self):
        '''Creates and serializes a delimited RpcRequestHeaderProto message.'''
        rpcheader = RpcRequestHeaderProto()
        rpcheader.rpcKind = 2  # rpcheaderproto.RpcKindProto.Value('RPC_PROTOCOL_BUFFER')
        rpcheader.rpcOp = 0  # rpcheaderproto.RpcPayloadOperationProto.Value('RPC_FINAL_PACKET')
        rpcheader.callId = self.call_id
        rpcheader.retryCount = -1
        rpcheader.clientId = self.client_id[0:16]

        if self.call_id == -3:
            self.call_id = 0
        else:
            self.call_id += 1

        # Serialize delimited
        s_rpcHeader = rpcheader.SerializeToString()
        log_protobuf_message("RpcRequestHeaderProto (len: %d)" % (len(s_rpcHeader)), rpcheader)
        return s_rpcHeader

    def create_connection_context(self):
        '''Creates and seriazlies a IpcConnectionContextProto (not delimited)'''
        context = IpcConnectionContextProto()
        context.userInfo.effectiveUser = self.effective_user
        context.protocol = self.context_protocol

        s_context = context.SerializeToString()
        log_protobuf_message("RequestContext (len: %d)" % len(s_context), context)
        return s_context

    def send_rpc_message(self, method, request):
        '''Sends a Hadoop RPC request to the NameNode.
        The IpcConnectionContextProto, RpcPayloadHeaderProto and HadoopRpcRequestProto
        should already be serialized in the right way (delimited or not) before
        they are passed in this method.
        The Hadoop RPC protocol looks like this for sending requests:
        When sending requests
        +---------------------------------------------------------------------+
        |  Length of the next three parts (4 bytes/32 bit int)                |
        +---------------------------------------------------------------------+
        |  Delimited serialized RpcRequestHeaderProto (varint len + header)   |
        +---------------------------------------------------------------------+
        |  Delimited serialized RequestHeaderProto (varint len + header)      |
        +---------------------------------------------------------------------+
        |  Delimited serialized Request (varint len + request)                |
        +---------------------------------------------------------------------+
        '''
        log.debug("############## SENDING ##############")

        #0. RpcRequestHeaderProto
        rpc_request_header = self.create_rpc_request_header()
        #1. RequestHeaderProto
        request_header = self.create_request_header(method)
        #2. Param
        param = request.SerializeToString()
        if log.getEffectiveLevel() == logging.DEBUG:
            log_protobuf_message("Request", request)

        rpc_message_length = len(rpc_request_header) + encoder._VarintSize(len(rpc_request_header)) + \
                             len(request_header) + encoder._VarintSize(len(request_header)) + \
                             len(param) + encoder._VarintSize(len(param))

        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug("RPC message length: %s (%s)" % (rpc_message_length, format_bytes(struct.pack('!I', rpc_message_length))))
        self.write(struct.pack('!I', rpc_message_length))

        self.write_delimited(rpc_request_header)
        self.write_delimited(request_header)
        self.write_delimited(param)

    def create_request_header(self, method):
        header = RequestHeaderProto()
        header.methodName = method.name
        header.declaringClassProtocolName = self.context_protocol
        header.clientProtocolVersion = 1

        s_header = header.SerializeToString()
        log_protobuf_message("RequestHeaderProto (len: %d)" % len(s_header), header)
        return s_header

    def recv_rpc_message(self):
        '''Handle reading an RPC reply from the server. This is done by wrapping the
        socket in a RcpBufferedReader that allows for rewinding of the buffer stream.
        '''
        log.debug("############## RECVING ##############")
        byte_stream = RpcBufferedReader(self.sock)
        return byte_stream

    def get_length(self, byte_stream):
        ''' In Hadoop protobuf RPC, some parts of the stream are delimited with protobuf varint,
        while others are delimited with 4 byte integers. This reads 4 bytes from the byte stream
        and retruns the length of the delimited part that follows, by unpacking the 4 bytes
        and returning the first element from a tuple. The tuple that is returned from struc.unpack()
        only contains one element.
        '''
        length = struct.unpack("!i", byte_stream.read(4))[0]
        log.debug("4 bytes delimited part length: %d" % length)
        return length

    def parse_response(self, byte_stream, response_class):
        '''Parses a Hadoop RPC response.
        The RpcResponseHeaderProto contains a status field that marks SUCCESS or ERROR.
        The Hadoop RPC protocol looks like the diagram below for receiving SUCCESS requests.
        +-----------------------------------------------------------+
        |  Length of the RPC resonse (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Delimited serialized RpcResponseHeaderProto              |
        +-----------------------------------------------------------+
        |  Serialized delimited RPC response                        |
        +-----------------------------------------------------------+
        In case of an error, the header status is set to ERROR and the error fields are set.
        '''

        log.debug("############## PARSING ##############")
        log.debug("Payload class: %s" % response_class)

        # Read first 4 bytes to get the total length
        len_bytes = byte_stream.read(4)
        total_length = struct.unpack("!I", len_bytes)[0]
        log.debug("Total response length: %s" % total_length)

        header = RpcResponseHeaderProto()
        (header_len, header_bytes) = get_delimited_message_bytes(byte_stream)

        log.debug("Header read %d" % header_len)
        header.ParseFromString(header_bytes)
        log_protobuf_message("RpcResponseHeaderProto", header)

        if header.status == 0:
            log.debug("header: %s, total: %s" % (header_len, total_length))
            if header_len >= total_length:
                return
            response = response_class()
            response_bytes = get_delimited_message_bytes(byte_stream, total_length - header_len)[1]
            if len(response_bytes) > 0:
                response.ParseFromString(response_bytes)
                if log.getEffectiveLevel() == logging.DEBUG:
                    log_protobuf_message("Response", response)
                return response
        else:
            self.handle_error(header)

    def handle_error(self, header):
        raise RpcError(header.exceptionClassName, header.errorMsg)

    def close_socket(self):
        '''Closes the socket and resets the channel.'''
        log.debug("Closing socket")
        if self.sock:
            try:
                self.sock.close()
            except:
                pass

            self.sock = None

    def CallMethod(self, method, controller, request, response_class, done):
        '''Call the RPC method. The naming doesn't confirm PEP8, since it's
        a method called by protobuf
        '''
        try:
            self.validate_request(request)

            if not self.sock:
                self.get_connection(self.host, self.port)

            self.send_rpc_message(method, request)

            byte_stream = self.recv_rpc_message()
            return self.parse_response(byte_stream, response_class)
        except RpcError:  # Raise a request error, but don't close the socket
            raise
        except Exception:  # All other errors close the socket
            self.close_socket()
            raise
