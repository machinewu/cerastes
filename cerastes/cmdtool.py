#!/usr/bin/env python
# encoding: utf-8

"""Cerastes: a command line interface for Yarn.
Usage:
  cerastes rmadmin -c CLUSTER [-v...]
  cerastes appclient -c CLUSTER [-v...]
  cerastes mradmin -c CLUSTER [-v...]
  cerastes mrclient -c CLUSTER [-v...]
  cerastes history -c CLUSTER [-v...]
  cerastes -V | -h
Commands:
  rmadmin                       Start a resource manager admin interactive
                                shell via the python interpreter. Perform
                                Yarn administration and HA operations.
  appclient                     Start a resource manager application client via
                                the python interpreter. Perform Application 
                                operations like query application list and get
                                statistics about yarn applications.
  mradmin                       Start a map-reduce history server admin client
                                allows performing operations like referehing 
                                the history server ...
  mrclient                      Start a map-reduce history server client. Performs
                                operations like query map reduce jobs and get statistics.
  history                       Starts a timeline server client if enabled.
                                This centralizes all informations about yarn applications
                                history.
Options:
  -L --log                      Show path to current log file and exit.
  -V --version                  Show version and exit.
  -c CLUSTER                    The name of the cluster to use.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
Examples:
  cerastes rmadmin -c prod
  cerastes appclient -c staging
  cerastes history -c prod
Cerastes exits with return status 1 if an error occurred and 0 otherwise.
"""

from . import __version__
from cerastes.config import CerastesConfig
from cerastes.client import YarnRmanAdminClient, YarnRmanApplicationClient, MrAdminClient, MrClient, YarnHistoryServerClient
from docopt import docopt
from threading import Lock
import logging as lg
import os.path as osp
import requests as rq
import os
import sys
import glob

def configure_client(args, client_class):
  """Instantiate configuration from arguments dictionary.
  :param args: Arguments returned by `docopt`.
  :param config: CLI configuration, used for testing.
  If the `--log` argument is set, this method will print active file handler
  paths and exit the process.
  """
  
  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)

  levels = {0: lg.CRITICAL, 1: lg.ERROR, 2: lg.WARNING, 3: lg.INFO}

  # Configure stream logging if applicable
  stream_handler = lg.StreamHandler()
  # This defaults to zero
  stream_log_level=levels.get(args['--verbose'], lg.DEBUG)
  stream_handler.setLevel(stream_log_level)

  fmt = '%(levelname)s\t%(message)s'
  stream_handler.setFormatter(lg.Formatter(fmt))
  logger.addHandler(stream_handler)

  config = CerastesConfig()

  # configure file logging if applicable
  handler = config.get_log_handler()
  logger.addHandler(handler)

  return config.get_client(args['-c'],client_class)

def main(argv=None, client=None):
  """Entry point.
  :param argv: Arguments list.
  :param client: For testing.
  """
  args = docopt(__doc__, argv=argv, version=__version__)
  if args['rmadmin']:
    client = configure_client(args, YarnRmanAdminClient)
    class_name = "RM ADMIN"
  elif args['appclient']:
    client = configure_client(args, YarnRmanApplicationClient)
    class_name = "RM APPLICATIONS"
  elif args['mradmin']:
    client = configure_client(args, MrAdminClient)
    class_name = "MAP-REDUCE ADMIN"
  elif args['mrclient']:
    client = configure_client(args, MrClient)
    class_name = "MAP-REDUCE"
  elif args['history']:
    client = configure_client(args, YarnHistoryServerClient)
    class_name = "HISTORY"

  banner = (
      '\n'
      '---------------------------------------------------\n'
      '------------------- CERASTES %s -6-----------------\n'
      '---- Welcome to YARN interactive python shell. ----\n'
      '- The YARN %s client is available as `CLIENT`. -\n'
      '--------------------- Enjoy ! ---------------------\n'
      '---------------------------------------------------\n'
      '\n'
  ) % (__version__,class_name)

  namespace = {'CLIENT': client}
  
  try:
    from IPython import embed
  except ImportError:
    from code import interact
    interact(banner=banner, local=namespace)
  else:
    embed(banner1=banner, user_ns=namespace)

if __name__ == '__main__':
  main()

