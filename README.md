[![pypi](https://badge.fury.io/py/cerastes.svg)](https://badge.fury.io/py/cerastes)

Cerastes
==================================

Cerastes is a pure python Client and library for Hadoop Yarn. The client uses RPC protobuf for communicating with Yarn services and provides a convinient python API for performing administration tasks on Yarn.

Functionalities
--------


* Implement ResourceManager Administration Protocol
* Implement ResourceManager Application Management Protocol
* Implement HistoryServer Applications monitoring Protocol
* Support both secure (Kerberos,Token) and insecure clusters
* Supports HA cluster and implements HA ResourceManager Administration Tasks
* Command line interface to interactively interact with YARN RCP apis on a python shell.
* Works on Hadoop 2.0.0, tested mainly against CDH 5.x


Getting started
---------------

Cerastes releases are available through pypi at <https://pypi.python.org/pypi/cerastes/>

To install simply run:

```bash
  $ pip install cerastes
```

USAGE
-------

Cerastes provides different Clients depending on the PRC Endpoint to communicate with, currently the supported client types are:
* YarnAdminClient : Yarn Resource Manager administration client (non HA).
* YarnHARMClient : Yarn Resource Manager HA administration client, this client only implements the HAService protocol and is limited to one Ressource Manager.
* YarnAdminHAClient: Yarn Resource Manager HA administration client, this client implements all yarn RM administration tasks and HA tasks (like failover).
* YarnRMApplicationClient: Yarn Resource Manager Application protocol, perform yarn applications management tasks. 


Currently the interactive python shell client is the easiest way to use cerastes:

```python 
>>> import cerastes.client as client
>>> RMCLIENT = client.YarnAdminClient(host="nn.hadoop.localdomain", port= 8033, use_sasl=True, yarn_rm_principal="yarn@HADOOP.LOCALDOMAIN", version=9)
>>> RMCLIENT.get_groups_for_user("yassine.azzouz")
>>> RMCLIENT.add_to_cluster_node_labels('test')
>>>
>>> HACLIENT = client.YarnAdminHAClient([{'host': "nn.hadoop.localdomain", 'port': 8033},{'host': "nn02.hadoop.localdomain", 'port': 8033}], use_sasl=True, yarn_rm_principal="yarn@HADOOP.LOCALDOMAIN", version=9)
>>> HACLIENT.explicit_failover(force=True)
True
>>>
>>> CLIENT = client.YarnHAAdminClient(host="nn.hadoop.localdomain", port=8033, use_sasl=True, yarn_rm_principal="yarn/nn.hadoop.localdomain@HADOOP.LOCALDOMAIN", version=9)
>>> CLIENT.get_service_status()
{'state': 'ACTIVE', 'readyToBecomeActive': True}
>>>
>>> APPCLIENT = client.YarnRMApplicationClient([{'host': "nn.hadoop.localdomain", 'port': 8032}], use_sasl=True, yarn_rm_principal="yarn/nn.hadoop.localdomain@HADOOP.LOCALDOMAIN", version=9)
>>> APPCLIENT.get_applications()

```

Contributing
------------

I'd love to hear what you think about cerastes and appreciate any idea or suggestion, Pull requests are also very welcome!
