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

__all__ = ['wait_for_node', 'NodeSynchStrategy', 'NodeSynchTimeout']

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

def wait_for_node(resolved_node_definition, instance_data, infraprocessor,
                  poll_delay=10, timeout=None, cancel_event=None):
    """
    Wait for the creation of the node using the appropriate
    :class:`NodeSynchStrategy`.

    May raise an exception, if node creation fails (depending on synch type).

    :param resolved_node_definition: The resolved node definition, serving as
        contextual information.
    :param instance_data: Instance information.
    :param infraprocessor: The InfrastructureProcessor calling this method.
        Used for contextual information and to access OCCO facilities like the
        CloudHandler.
    :param int poll_delay: Time (seconds) to wait between polls.
    :param int timeout: Timeout in seconds. If :data:`None` or 0, there will
        be no timeout. This is approximate timeout, the actual timeout will
        happen somwhere between ``(start+timeout)`` and
        ``(start+timeout+poll_delay)``.
    :param cancel_event: The polling will be cancelled when this event is set.
    :type cancel_event: :class:`threading.Event`
    """
    synch_type = util.coalesce(
        # Can be specified by the node definition (implementation).
        # A node definition based on legacy material can even define an ad-hoc
        # strategy for that sole node type:
        resolved_node_definition.get('synch_strategy'),
        # There can be a generic strategy for a node implementation type:
        resolved_node_definition.get('implementation_type'),
        # Default strategy:
        'basic')
    if not NodeSynchStrategy.has_backend(synch_type):
        synch_type = 'basic'

    synch = NodeSynchStrategy.instantiate(
        synch_type, resolved_node_definition, instance_data, infraprocessor)

    node_id = resolved_node_definition['node_id']
    log.info('Waiting for node %r to become ready using %r strategy.',
             node_id, synch_type)

    import time
    while not synch.is_ready():
        log.debug('Node %r is not ready, waiting %r seconds.',
                  node_id, poll_delay)
        if not sleep(poll_delay, cancel_event):
            log.debug('Waiting for node %r has been cancelled.', node_id)
            return

    log.info('Node %r is ready.', node_id)

class NodeSynchStrategy(factory.MultiBackend):
    def __init__(self, resolved_node_definition, instance_data, infraprocessor):
        self.resolved_node = resolved_node_definition
        self.instance_data = instance_data
        self.infraprocessor = infraprocessor
        import occo.infobroker
        self.ib = occo.infobroker.main_info_broker
        self.node_id = instance_data['node_id']

    def is_ready(self):
        raise NotImplementedError()

@factory.register(NodeSynchStrategy, 'basic')
class BasicNodeSynchStrategy(NodeSynchStrategy):
    def is_ready(self):
        return \
            self.status_ready() \
            and self.attributes_ready()

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
