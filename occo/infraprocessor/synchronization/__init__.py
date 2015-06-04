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
    # Can be specified by the node definition (implementation).
    # A node definition based on legacy material can even define an ad-hoc
    # strategy for that sole node type:
    synchstrat = resolved_node_definition.get('synch_strategy')
    if synchstrat:
        # The synch strategy may be parameterizable (like 'basic' is)
        key = synchstrat['protocol'] \
            if isinstance(synchstrat, dict) \
            else synchstrat
        if not NodeSynchStrategy.has_backend(key):
            # If specified, but unknown, that is an error (typo or misconfig.)
            raise ValueError('Unknown synch_strategy', key)
    else:
        # No special synch strategy has been defined.
        # Trying a generic synch strategy for the implementation type
        key = resolved_node_definition.get('implementation_type'),
        if not NodeSynchStrategy.has_backend(key):
            # There is no generic synch strategy for the implementation type
            # Using default synch strategy
            key = 'basic'

    return key

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
        self.resolved_node_definition = resolved_node_definition
        self.instance_data = instance_data
        import occo.infobroker
        self.ib = occo.infobroker.main_info_broker
        self.node_id = instance_data['node_id']
        self.infra_id = resolved_node_definition['environment_id']

    def generate_report(self):
        raise NotImplementedError()

    def is_ready(self):
        raise NotImplementedError()

from occo.infraprocessor.synchronization.primitives import *
basic_status = StatusTag('Generic status information')

@factory.register(NodeSynchStrategy, 'basic')
class BasicNodeSynchStrategy(CompositeStatus, NodeSynchStrategy):
    def is_ready(self):
        return self.get_composite_status(basic_status)

    @status_component('Backend status', basic_status)
    def status_ready(self):
        log.debug('Checking node status for %r', self.node_id)
        status = self.ib.get('node.state', self.instance_data)
        log.info('Status of node %r is %r', self.node_id, status)
        return 'running:ready' == status

    def get_kwargs(self):
        if not hasattr(self, 'kwargs'):
            self.kwargs = self.resolved_node_definition.get(
                'synch_strategy', dict())
        return self.kwargs

    def make_node_spec(self):
        return dict(infra_id=self.infra_id, node_id=self.node_id)
    def get_node_address(self):
        return self.ib.get('node.address', **self.make_node_spec())

    def resolve_url(self, fmt):
        data = dict(
            node_id=self.instance_data['node_id'],
            ibget=self.ib.get,
            instance_data=self.instance_data,
            variables=self.node_description['variables'],
            addr=self.get_node_address(),
        )
        import jinja2
        tmp = jinja2.Template(fmt)
        return tmp.render(data)

    @status_component('Network reachability', basic_status)
    def reachable(self):
        if self.get_kwargs().get('ping', True):
            log.debug('Checking node reachability (%s)', self.node_id)
            return self.ib.get('synch.node_reachable', **self.make_node_spec())

    @status_component('URL Availability', basic_status)
    def urls_ready(self):
        urls = self.get_kwargs().get('urls', list())
        for fmt in urls:
            log.debug('Checking URL availability: %r', i)
            url = self.resolve_url(fmt)
            available = self.ib.get('synch.site_available', url)
            if not available:
                log.info('Site %r is still not available.', url)
                return False
            else:
                log.info('Site %r has become available.', url)

        return True

    @status_component('Attribute Availability', basic_status)
    def attributes_ready(self):
        synch_attrs = self.resolved_node_definition.get('synch_attrs')
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
