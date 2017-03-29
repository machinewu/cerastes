#!/usr/bin/python

import re

class YarnError(Exception):
  """Base error class.
  :param message: Error message.
  :param args: optional Message formatting arguments.
  """
  def __init__(self, message, *args):
    super(YarnError, self).__init__(message % args if args else message)

class AuthorizationException(YarnError):
  def __init__(self, message, *args):
    super(AuthorizationException, self).__init__(message % args if args else message)

class StandbyError(YarnError):
  def __init__(self, message, *args):
    super(StandbyError, self).__init__(message % args if args else message)

class RpcError(Exception):
  """Base Rpc error class.
  :param message: Error message.
  :param args: optional Message formatting arguments.
  """
  def __init__(self, class_name, message):
    self.class_name = class_name
    # keep a copy of the raw message
    self.raw_message = message
    self.message = ""
    '''
      Parse java stacktrace to identify the relevent error message
      Typically java stacktrace have the following format:
        - Exception name, and optionally a message
        - The subsequent stack trace will begin with one of the following:
            * "\tat" (tab + at)
            * "Caused by: "
            * "\t... <number of frames in the stack not shown> more"
      The part that are interesting are the first line and the caused by lines
      we just ignore the rest.
    '''
    pattern = re.compile(r'^(\t|\s)*(at|\.\.\.)\s')
    for line in message.splitlines():
       if re.match(pattern, line):
           continue
       else:
           self.message = ' '.join([self.message, line])
    super(RpcError, self).__init__(self.message)

class RpcConnectionError(RpcError):
  def __init__(self, message, *args):
    super(RpcConnectionError, self).__init__("", message % args if args else message)

class RpcAuthenticationError(RpcError):
  def __init__(self, message, *args):
    super(RpcAuthenticationError, self).__init__("", message % args if args else message)

class MalformedRpcRequestError(RpcError):
  def __init__(self, message, *args):
    super(MalformedRpcRequestError, self).__init__("", message % args if args else message)

class RpcSaslError(RpcError):
  def __init__(self, message, *args):
    super(RpcSaslError, self).__init__("", message % args if args else message)

class RpcBufferError(RpcError):
  def __init__(self, message, *args):
    super(RpcBufferError, self).__init__("", message % args if args else message)
