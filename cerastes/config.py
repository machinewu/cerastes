#!/usr/bin/env python
# encoding: utf-8

import ast
from cerastes.client import YarnAdminClient, YarnAdminHAClient
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

class YarnConfig(object):

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
      path = osp.join(gettempdir(), 'cerastes.log')
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

  def parse_common_arguments(self, cluster, params):

       if 'version' in cluster:
         params['version'] = cluster['version']

       if 'effective_user' in cluster:
         params['effective_user'] = cluster['effective_user']

       if 'yarn_rm_principal' in cluster:
         params['yarn_rm_principal'] = cluster['yarn_rm_principal']

       if 'use_sasl' in cluster:
         params['use_sasl'] = cluster['use_sasl']

       if 'sock_connect_timeout' in cluster:
         params['sock_connect_timeout'] = cluster['sock_connect_timeout']

       if 'sock_request_timeout' in cluster:
         params['sock_request_timeout'] = cluster['sock_request_timeout']

       return params

  def get_rm_admin_client(self, cluster_name, **kwargs):
      """
      :param cluster_name: The client to look up. If the cluster name does not
        exist and exception will be raised.
      :param kwargs: additional arguments can be used to overwrite or add some 
        parameters defined in the configuration file.
      """
      VALID_EXTRA_ARGS = ['resourcemanagers', 'version', 'effective_user', 'use_sasl', 'yarn_rm_principal', 'sock_connect_timeout', 'sock_request_timeout']
      params = {}
      for cluster in self.config['clusters']:
        if cluster['name'] == cluster_name:

            params = self.parse_common_arguments(cluster,params)

            if 'resourcemanagers' in kwargs:
              resourcemanagers = kwargs['resourcemanagers']
              del kwargs['resourcemanagers']
            elif 'resourcemanagers' in cluster:
              resourcemanagers = cluster['resourcemanagers']
            else:
              raise YarnError("resourcemanagers configuration missing")

            if len(resourcemanagers) > 1:
                services = []
                # The cluster is in HA
                for service in resourcemanagers:
                  services.extend([{'host': service['hostname'], 'port': service['administration_port']}])
                params['services'] = services
                client_class = YarnAdminHAClient
            elif len(resourcemanagers) == 1:
                params['host'] = resourcemanagers[0]['hostname']
                params['port'] = resourcemanagers[0]['administration_port']
                client_class = YarnAdminClient
            else:
                raise YarnError("resourcemanagers configuration seems wrong")

            # set overwrite arguments            
            for extra_option in kwargs:
              if extra_option not in VALID_EXTRA_ARGS:
                raise YarnError("invalid configuration option %s" % extra_option)
              params[extra_option] = kwargs[extra_option]

            return client_class(**params)

      # the name does not exist
      raise YarnError('Cluster %s is not defined in configuration file.' % cluster_name)
