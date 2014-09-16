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

class DummyInfoBroker(object):
    def get(key):
        pass

class DummyServiceComposer(object):
    def register_node(self, node):
        log.debug("[SC] Registering node: %r", node)
    def drop_node(self, node_id):
        log.debug("[SC] Dropping node '%s'", node_id)
    def create_environment(self, environment_id):
        log.debug("[SC] Creating environment '%s'", environment_id)
    def drop_environment(self, environment_id):
        log.debug("[SC] Dropping environment '%s'", environment_id)

class DummyCloudHandler(object):
    def create_node(self, node):
        log.debug("[CH] Creating node: %r", node)
    def drop_node(self, node_id):
        log.debug("[CH] Dropping node '%s'", node_id)
