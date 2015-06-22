#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import occo.util.config as config
import occo.util as util
import logging
import logging.config
import occo.infobroker as ib

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
    service_composer.aux_data : {}
    node.resource.address: null
    node.resource.ip_address: null
    node.state: running:ready
    node.definition:
        implementation_type: chef+cloudinit
        service_composer_id: null
        backend_id: null
        synch_strategy:
            protocol: basic
            ping: false
        attributes:
            attr1: 1
            attr2:
              - "{{ find_node_id('preexisting_node') }}"
              - attr4
    """)

class DummyNode(dict):
    def __init__(self, environment_id, force_id=None):
        self['environment_id'] = environment_id
        self['user_id'] = 0
        self['type'] = 'dummynode'
        self['name'] = 'dummynode'
        if force_id:
            self['node_id'] = force_id
        self._started = False
    @property
    def started(self):
        return self.get('_started', False)

@ib.provider
class DummyInfoBroker(ib.InfoProvider):
    def __init__(self, main_info_broker=True):
        ib.InfoProvider.__init__(self, main_info_broker=main_info_broker)
        self.environments = dict()
        self.node_lookup = dict(preexisting_node=['preexisting node'])

    def get(self, key, *args, **kwargs):
        if self.can_get(key):
            return ib.InfoProvider.get(self, key, *args, **kwargs)
        else:
            return dummydata[key]

    @ib.provides('node.find')
    def find_node(self, infra_id, name):
        try:
            return [self.node_lookup[name]]
        except KeyError:
            return None

    def __repr__(self):
        log.info('%r', self.environments)
        nodelist_repr = lambda nodelist: ', '.join(
            "{0}_{1}".format(n['node_id'], n.get('_started', False))
            for n in nodelist)
        envlist_repr = list('%s:[%s]'%(k, nodelist_repr(v))
                            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)

class DummyServiceComposer(object):
    def __init__(self, infobroker):
        self.ib = infobroker
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node)
        self.ib.environments[node['environment_id']].append(node)
        self.ib.node_lookup[node['node_id']] = node
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_node(self, node_id):
        log.debug("[SC] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        env_id = node['environment_id']
        self.ib.environments[env_id] = list(
            i for i in self.ib.environments[env_id]
            if i['node_id'] != node_id)
        del self.ib.node_lookup[node_id]
        log.debug("[SC] Done - '%r'", self.ib)

    def create_environment(self, environment_id):
        log.debug("[SC] Creating environment '%s'", environment_id)
        self.ib.environments.setdefault(environment_id, [])
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)
        del self.ib.environments[environment_id]
        log.debug("[SC] Done - '%r'", self.ib)

class DummyCloudHandler(object):
    def __init__(self, infobroker):
        self.ib = infobroker
    def create_node(self, node):
        log.debug("[CH] Creating node: %r", node)
        node['_started'] = True
        log.debug("[SC] Done - '%r'", self.ib)

    def drop_node(self, node_id):
        log.debug("[CH] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        node['_started'] = False
        log.debug("[SC] Done - '%r'", self.ib)
