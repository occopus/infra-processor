#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import occo.util.config as config
import occo.util as util
import logging
import logging.config

cfg = config.DefaultYAMLConfig(util.rel_to_file('test.yaml'))

logging.config.dictConfig(cfg.logging)

log = logging.getLogger('occo.unittests')

import uuid
def uid():
    return str(uuid.uuid4())

dummydata = {
    'node.definition' : {
        'implementation_type' : 'cooked'
    }
}

class DummyNode(dict):
    def __init__(self, environment_id, force_id=None):
        self['environment_id'] = environment_id
        self['type'] = 'dummynode'
        if force_id:
            self['id'] = force_id
        self._started = False
    @property
    def started(self):
        return self._started
    def __repr__(self):
        return '%s_%r'%(self['id'], self.started)

class DummyInfoBroker(object):
    def __init__(self, main_info_broker=False):
        import occo.infobroker as ib
        ib.main_info_broker = self
        self.environments = dict()
        self.node_lookup = dict()
    def get(self, key, *args, **kwargs):
        return dummydata[key]
    def __repr__(self):
        log.info('%r', self.environments)
        nodelist_repr = lambda nodelist: ', '.join(repr(n) for n in nodelist)
        envlist_repr = list('%s:[%s]'%(k, nodelist_repr(v))
                            for (k, v) in self.environments.iteritems())
        return ' '.join(envlist_repr)

class DummyServiceComposer(object):
    def __init__(self, infobroker):
        self.ib = infobroker
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node)
        self.ib.environments[node['environment_id']].append(node)
        self.ib.node_lookup[node['id']] = node
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_node(self, node_id):
        log.debug("[SC] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        env_id = node['environment_id']
        self.ib.environments[env_id] = list(
            i for i in self.ib.environments[env_id]
            if i['id'] != node_id)
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
        node._started = True
        log.debug("[SC] Done - '%r'", self.ib)

    def drop_node(self, node_id):
        log.debug("[CH] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        node._started = False
        log.debug("[SC] Done - '%r'", self.ib)
