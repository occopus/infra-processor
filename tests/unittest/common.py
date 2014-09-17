#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import occo.util.config as config
import logging
import logging.config

with open('test.yaml') as f:
    cfg = config.DefaultYAMLConfig(f)

logging.config.dictConfig(cfg.logging)

log = logging.getLogger('occo.unittests')

class DummyNode(object):
    def __init__(self, id, environment_id):
        self._id = id
        self._environment_id = environment_id
        self._started = False
    @property
    def id(self):
        return self._id
    @property
    def environment_id(self):
        return self._environment_id
    @property
    def started(self):
        return self._started
    def __repr__(self):
        return '(%s_%r)'%(self.id, self.started)

class DummyInfoBroker(object):
    def __init__(self):
        self.environments = dict()
        self.node_lookup = dict()
    def get(key):
        pass
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
        self.ib.environments[node.environment_id].append(node)
        self.ib.node_lookup[node.id] = node
        log.debug("[SC] Done - '%r'", self.ib)
    def drop_node(self, node_id):
        log.debug("[SC] Dropping node '%s'", node_id)
        node = self.ib.node_lookup[node_id]
        env_id = node.environment_id
        self.ib.environments[env_id] = list(
            i for i in self.ib.environments[env_id]
            if i.id != node_id)
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
