### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

import occo.util.config as config
import occo.util as util
import logging
import logging.config
import occo.infobroker as ib
import occo.infraprocessor.synchronization.primitives as sp

cfg = config.DefaultYAMLConfig(util.rel_to_file('test.yaml'))

logging.config.dictConfig(cfg.logging)

log = logging.getLogger('occo.unittests')

import uuid
def uid():
    return str(uuid.uuid4())

import yaml
dummydata = yaml.load(
    """
    backends.auth_data : {}
    config_manager.aux_data : {}
    node.resource.address: null
    node.resource.ip_address: null
    node.state: ready
    nodedefs:
        dummynode: &DN
            implementation_type: chef+cloudinit
            config_manager_id: null
            backend_id: null
            health_check:
                protocol: basic
                ping: false
            attributes:
                attr1: 1
                attr2:
                  - "{{ find_node_id('preexisting_node') }}"
                  - attr4
        synch1:
            <<: *DN
            health_check: basic
        synch_bad:
            <<: *DN
            health_check: nonexistent
    """)

class DummyNode(dict):
    def __init__(self, infra_id, force_id=None,
                 node_type='dummynode', node_name='dummynode'):
        self['infra_id'] = infra_id
        self['user_id'] = 0
        self['type'] = node_type
        self['name'] = node_name
        if force_id:
            self['node_id'] = force_id
        self._started = False
    @property
    def started(self):
        return self.get('_started', False)

@ib.provider
class DefaultIB(ib.InfoProvider):
    def get(self, key, *args, **kwargs):
        return dummydata[key]
    def can_get(self, key):
        return True

@ib.provider
class DummyInfoBroker(ib.InfoRouter):
    def __init__(self):
        ib.InfoRouter.__init__(self)
        self.environments = dict()
        self.node_lookup = dict(preexisting_node=['preexisting node'])
        synch = sp.SynchronizationProvider()
        synch.dry_run = True
        self.sub_providers = [
            synch, DefaultIB()
        ]

    @ib.provides('node.find')
    def find_node(self, infra_id, name):
        try:
            return [self.node_lookup[name]]
        except KeyError:
            return None

    @ib.provides('node.definition')
    def nodedef(self, node_type, preselected_backend_ids, strategy, **kwargs):
        return dummydata['nodedefs'][node_type]

    def __repr__(self):
        log.info('%r', self.environments)
        nodelist_repr = lambda nodelist: ', '.join(
            "{0}_{1}".format(n['node_id'], n.get('_started', False))
            for n in nodelist)
        envlist_repr = list('{0}:[{1}]'.format(k, nodelist_repr(v))
                            for (k, v) in self.environments.items())
        return ' '.join(envlist_repr)

class DummyConfigManager(object):
    def __init__(self):
        self.ib = ib.main_info_broker
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node)
        self.ib.environments[node['infra_id']].append(node)
        self.ib.node_lookup[node['node_id']] = node
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_node(self, node):
        node_id = node['node_id']
        log.debug("[SC] Dropping node %r", node_id)
        node = self.ib.node_lookup[node_id]
        infra_id = node['infra_id']
        self.ib.environments[infra_id] = list(
            i for i in self.ib.environments[infra_id]
            if i['node_id'] != node_id)
        del self.ib.node_lookup[node_id]
        log.debug("[SC] Done - '%r'", self.ib)

    def create_infrastructure(self, infra_id):
        log.debug("[SC] Creating environment %r", infra_id)
        self.ib.environments.setdefault(infra_id, [])
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_infrastructure(self, infra_id):
        log.debug("[SC] Dropping environment %r", infra_id)
        del self.ib.environments[infra_id]
        log.debug("[SC] Done - '%r'", self.ib)

class DummyCloudHandler(object):
    def __init__(self):
        self.ib = ib.main_info_broker
    def create_node(self, node):
        log.debug("[CH] Creating node: %r", node)
        node['_started'] = True
        log.debug("[SC] Done - '%r'", self.ib)

    def drop_node(self, node):
        node_id = node['node_id']
        log.debug("[CH] Dropping node %r", node_id)
        node = self.ib.node_lookup[node_id]
        node['_started'] = False
        log.debug("[SC] Done - '%r'", self.ib)
