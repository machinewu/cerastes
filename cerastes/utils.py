
from threading import Lock
import logging

class SyncServicesList(object):
  def __init__(self, services):
    self.services = services
    self.lock = Lock()

  def __repr__(self):
    return self.services.__repr__()

  def get_active_host(self):
    with self.lock:
        return self.services[0]

  def get_host_count(self):
    return len(self.services)

  def switch_active_host(self, service):
    with self.lock:
      if service['host'] != self.services[0]['host']:
        # apparently some other process already switched that namenode
        return self.services[0]
      else:
        current_active= self.services.pop(0)
        self.services.append(current_active)
        return self.services[0]
