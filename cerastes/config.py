#!/usr/bin/env python
# encoding: utf-8

import ast
from cerastes.client import YarnConfig, YarnRmanAdminClient, YarnRmanApplicationClient
from cerastes.errors import YarnError
from pkg_resources import resource_string
from functools import wraps
from imp import load_source
from logging.handlers import TimedRotatingFileHandler
from tempfile import gettempdir
from copy import deepcopy
import logging as lg
import os
import os.path as osp
import jsonschema as js
import sys
import json

_logger = lg.getLogger(__name__)

class NullHandler(lg.Handler):
  """Pass-through logging handler.
  This is required for python <2.7.
  """

class CerastesConfig(object):

  default_path = osp.expanduser('~/.cerastes.cfg')

  def __init__(self, path=None):
      self.path = path or os.getenv('CERASTES_CONFIG', self.default_path)
      if osp.exists(self.path):
        try:
          self.config = json.loads(open(self.path).read())
          self.schema = json.loads(resource_string(__name__, 'resources/config_schema.json'))
          try:
            js.validate(self.config, self.schema)
          except js.ValidationError as e:
            print e.message
          except js.SchemaError as e:
            print e

        except Exception as e:
          raise YarnError('Invalid configuration file %r.', self.path)

        _logger.info('Instantiated configuration from %r.', self.path)
      else:
        raise YarnError('Invalid configuration file %r.', self.path)


  def get_log_handler(self):
      """Configure and return file logging handler."""
      path = osp.join(osp.expanduser("~"), 'cerastes.log')
      level = lg.DEBUG
      
      if 'configuration' in self.config:
        configuration = self.config['configuration']
        if 'logging' in configuration:
          logging_config = configuration['logging']
          if 'disable' in logging_config and logging_config['disable'] == True:
            return NullHandler()
          if 'path' in logging_config:
            path = logging_config['path'] # Override default path.
          if 'level' in logging_config:
            level = getattr(lg, logging_config['level'].upper())

      log_handler = TimedRotatingFileHandler(
          path,
          when='midnight', # Daily backups.
          backupCount=5,
          encoding='utf-8',
      )
      fmt = '%(asctime)s\t%(name)-16s\t%(levelname)-5s\t%(message)s'
      log_handler.setFormatter(lg.Formatter(fmt))
      log_handler.setLevel(level)
      return log_handler

  def get_client(self, cluster_name, client_class, **kwargs):
      """
      :param cluster_name: The client to look up. If the cluster name does not
        exist and exception will be raised.
      :param kwargs: additional arguments can be used to overwrite or add some 
        parameters defined in the configuration file.
      """
      for cluster in self.config['clusters']:
        if cluster['name'] == cluster_name:
            return client_class( YarnConfig(**cluster), **kwargs)

      # the name does not exist
      raise YarnError('Cluster %s is not defined in configuration file.' % cluster_name)

  def get_rmadmin_client(self, cluster_name, **kwargs):
      return self.get_client(cluster_name, YarnRmanAdminClient, **kwargs)

  def get_rmapp_client(self, cluster_name, **kwargs):
      return self.get_client(cluster_name, YarnRmanApplicationClient, **kwargs)
