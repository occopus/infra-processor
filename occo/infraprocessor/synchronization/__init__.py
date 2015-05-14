#
# Copyright (C) 2014 MTA SZTAKI
#

""" Abstract syncronization on node creation

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

Used by the BasicInfraProcessor.CreateNode command, this module provides an
abstract interface to wait for a node to be created. Being pluginable, it
enables defining multiple strategies for this task, among which the node
definition can choose.

"""

__import__('pkg_resources').declare_namespace(__name__)

__all__ = ['wait_for_node', 'NodeSynchStrategy', 'NodeSynchTimeout',
           'node_synch_type', 'get_synch_strategy']

import logging
import occo.util as util
import occo.util.factory as factory

log = logging.getLogger('occo.infraprocessor.synchronization')

class NodeSynchTimeout(Exception):
    pass

import time
def sleep(timeout, cancel_event):
    if cancel_event:
        cancel_event.wait(timeout=timeout)
        if cancel_event.isset():
            return False
    else:
        time.sleep(timeout)
    return True

def node_synch_type(resolved_node_definition):
    return util.coalesce(
        # Can be specified by the node definition (implementation).
        # A node definition based on legacy material can even define an ad-hoc
        # strategy for that sole node type:
        resolved_node_definition.get('synch_strategy'),
        # There can be a generic strategy for a node implementation type:
        resolved_node_definition.get('implementation_type'),
        # Default strategy:
        'basic')

def get_synch_strategy(instance_data):
    node_description = instance_data['node_description']
    resolved_node_definition = instance_data['resolved_node_definition']
    synch_type = node_synch_type(resolved_node_definition)
    log.info('Synchronization strategy for node %r is %r.',
             instance_data['node_id'], synch_type)

    return NodeSynchStrategy.instantiate(
        synch_type, node_description,
        resolved_node_definition, instance_data)


def wait_for_node(instance_data,
                  poll_delay=10, timeout=None, cancel_event=None):
    """
    Wait for the creation of the node using the appropriate
    :class:`NodeSynchStrategy`.

    May raise an exception, if node creation fails (depending on synch type).

    :param instance_data: Instance information.
    :param int poll_delay: Time (seconds) to wait between polls.
    :param int timeout: Timeout in seconds. If :data:`None` or 0, there will
        be no timeout. This is approximate timeout, the actual timeout will
        happen somwhere between ``(start+timeout)`` and
        ``(start+timeout+poll_delay)``.
    :param cancel_event: The polling will be cancelled when this event is set.
    :type cancel_event: :class:`threading.Event`
    """
    synch = get_synch_strategy(instance_data)

    node_id = instance_data['node_id']
    log.info('Waiting for node %r to become ready.', node_id)

    while not synch.is_ready():
        log.debug('Node %r is not ready, waiting %r seconds.',
                  node_id, poll_delay)
        if not sleep(poll_delay, cancel_event):
            log.debug('Waiting for node %r has been cancelled.', node_id)
            return

    log.info('Node %r is ready.', node_id)

class NodeSynchStrategy(factory.MultiBackend):
    def __init__(self,
                 node_description,
                 resolved_node_definition,
                 instance_data):
        self.node_description = node_description
        self.resolved_node = resolved_node_definition
        self.instance_data = instance_data
        import occo.infobroker
        self.ib = occo.infobroker.main_info_broker
        self.node_id = instance_data['node_id']
        self.infra_id = resolved_node_definition['environment_id']

    def generate_report(self):
        raise NotImplementedError()

    def is_ready(self):
        raise NotImplementedError()

@factory.register(NodeSynchStrategy, 'basic')
class BasicNodeSynchStrategy(NodeSynchStrategy):
    def is_ready(self):
        return \
            self.status_ready() \
            and self.attributes_ready()

    def generate_report(self):
        return [('Node status', self.status_ready()),
                ('Attributes ready', self.attributes_ready())]

    def status_ready(self):
        log.debug('Checking node status for %r', self.node_id)
        status = self.ib.get('node.state', self.instance_data)
        log.info('Status of node %r is %r', self.node_id, status)
        return 'running:ready' == status

    def attributes_ready(self):
        synch_attrs = self.resolved_node['synch_attrs']
        if not synch_attrs:
            # Nothing to synchronize upon
            return True

        ib, node_id = self.ib, self.node_id

        for attribute in synch_attrs:
            log.debug('Checking attribute availability: %r.', attribute)
            try:
                value = ib.get('node.attribute', node_id, attribute)
            except KeyError:
                log.info(
                    'Attribute %r is still unavailable (not found).', attribute)
                return False
            if value is None:
                log.info(
                    'Attribute %r is still unavailable (no value).', attribute)
                return False

        log.info('All attributes of node %r are available.', node_id)
        return True
