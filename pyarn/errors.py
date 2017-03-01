#!/usr/bin/python

class YarnError(Exception):

  """Base error class.
  :param message: Error message.
  :param args: optional Message formatting arguments.
  """

  def __init__(self, message, *args):
    super(YarnError, self).__init__(message % args if args else message)
