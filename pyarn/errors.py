#!/usr/bin/python
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
  """Base error class.
  :param message: Error message.
  :param args: optional Message formatting arguments.
  """
  def __init__(self, message, *args):
    super(RpcError, self).__init__(message % args if args else message)

class RpcAuthenticationError(RpcError):
  def __init__(self, message, *args):
    super(RpcAuthenticationError, self).__init__(message % args if args else message)

class MalformedRpcRequestError(RpcError):
  def __init__(self, message, *args):
    super(MalformedRpcRequestError, self).__init__(message % args if args else message)

class RpcSaslError(RpcError):
  def __init__(self, message, *args):
    super(RpcSaslError, self).__init__(message % args if args else message)

class RpcBufferError(RpcError):
  def __init__(self, message, *args):
    super(RpcBufferError, self).__init__(message % args if args else message)
