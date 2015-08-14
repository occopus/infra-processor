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

__all__ = ['wait_for_node', 'NodeSynchStrategy',
           'node_synch_type', 'get_synch_strategy']

import logging
import occo.util as util
from occo.exceptions.orchestration import *
import occo.util.factory as factory
import occo.constants.status as node_status
import occo.infobroker

log = logging.getLogger('occo.infraprocessor.synchronization')
ib = occo.infobroker.main_info_broker

import time, datetime
def sleep(timeout, cancel_event):
    """
    Sleeps  until the timeout is reached, or until cancelled through
    :param:`cancel_event`.
    """
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
        src = 'node_definitioni.synch_strategy'
        if not NodeSynchStrategy.has_backend(key):
            # If specified, but unknown, that is an error (typo or misconfig.)
            raise ValueError('Unknown synch_strategy', key)
    else:
        # No special synch strategy has been defined.
        # Trying a generic synch strategy for the implementation type
        key = resolved_node_definition.get('implementation_type'),
        src = 'node_definition.implementation_type'
        if not NodeSynchStrategy.has_backend(key):
            # There is no generic synch strategy for the implementation type
            # Using default synch strategy
            key, src = 'basic', 'default'

    log.debug('SynchStrategy protocol is %r (from %s)', key, src)
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

    if timeout:
        start_time = time.time()
        finish_time = start_time + timeout
        log.info(('Waiting for node %r to become ready with '
                  '%d seconds timeout. Deadline: %s'),
                 node_id,
                 timeout,
                 datetime.datetime.fromtimestamp(finish_time).isoformat())
    else:
        log.info('Waiting for node %r to become ready. No timeout.', node_id)

    while not synch.is_ready():
        if timeout and time.time() > finish_time:
            raise NodeCreationTimeOutError(
                    instance_data=instance_data,
                    reason=None,
                    msg=('Timeout ({0}s) in node creation!'
                         .format(timeout)))

        status = ib.get('node.state', instance_data)
        if status in [node_status.SHUTDOWN, node_status.FAIL]:
            raise NodeFailedError(instance_data, status)

        log.debug('Node %r is not ready, waiting %r seconds.',
                  node_id, poll_delay)
        if not sleep(poll_delay, cancel_event):
            log.debug('Waiting for node %r has been cancelled.', node_id)
            return

    log.info('Node %r is ready.', node_id)

class NodeSynchStrategy(factory.MultiBackend):
    """
    Abstract strategy to check whether a node is ready to be used.

    :param node_description: The node description as provided by the Compiler.
        It is the same as the input of the InfraProcessor's CreateNode command.

    :param resolved_node_definition: The node definition as resolved by the
        InfraProcessor. It is the same as the input of the CloudHandler and
        ServiceComposer's CreateNode commands.

    :param instance_data: The instance data as provided by the InfraProcessor
        after successfully creating a node.

    .. todo:: node_desc and resolved_node_def are a part of the instance data;
        thus, these should be factored out to simplify this interface.
    """
    def __init__(self,
                 node_description,
                 resolved_node_definition,
                 instance_data):
        self.node_description = node_description
        self.resolved_node_definition = resolved_node_definition
        self.instance_data = instance_data
        self.node_id = instance_data['node_id']
        self.infra_id = resolved_node_definition['infra_id']

    def generate_report(self):
        """
        Overridden in a derived class, generates a detailed report about the
        nodes status.
        """
        raise NotImplementedError()

    def is_ready(self):
        """
        Overridden in a derived class, determines if the node is ready to be
        used: the resource is ready and the services are in working order.

        As waiting for a node to become ready, the InfraProcessor will wait for
        this method to return :data:`True`.
        """
        raise NotImplementedError()

from occo.infraprocessor.synchronization.primitives import *
basic_status = StatusTag('Generic status information')

@factory.register(NodeSynchStrategy, 'basic')
class BasicNodeSynchStrategy(CompositeStatus, NodeSynchStrategy):
    """
    Default synchronization strategy. This strategy ensures the following
    properties of the node:
      - Status (as reported by the InfoBroker)
      - Network reachability (using ping)
      - URLs available (using a HEAD request)
      - Availability of attributes

    Of these, only the node status is checked unconditionally. All others are
    parameterizable.

    .. todo:: synch_attrs is now a part of the node definition - it should be
        moved to be a parameter of this NodeSynchStrategy.

    .. todo:: Configuring an instance is cumbersome (see :meth:`get_kwargs` et
        al.) It should be more user friendly.

    .. todo:: URLs available: the method should be parameterizable.
    """
    def is_ready(self):
        return self.get_composite_status(basic_status)

    @status_component('Backend status', basic_status)
    def status_ready(self):
        log.debug('Checking node status for %r', self.node_id)
        status = ib.get('node.state', self.instance_data)
        log.info('Status of node %r is %r', self.node_id, status)
        return status == node_status.READY

    def get_kwargs(self):
        """
        .. todo:: Make this more generic (not only BasicNodeSynchStrategy will
            be parameterizable.
        """
        if not hasattr(self, 'kwargs'):
            self.kwargs = self.resolved_node_definition.get(
                'synch_strategy', dict())
            if isinstance(self.kwargs, basestring):
                # synch_strategy has been specified as a non-parameterized
                # string.
                self.kwargs = dict()
        return self.kwargs

    def make_node_spec(self):
        return dict(infra_id=self.infra_id, node_id=self.node_id)

    def get_node_address(self):
        return ib.get('node.address', **self.make_node_spec())

    def resolve_url(self, fmt):
        """
        .. todo:: Document the data that can be used in the URL template.
        """
        data = dict(
            node_id=self.instance_data['node_id'],
            ibget=ib.get,
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
            return ib.get('synch.node_reachable', **self.make_node_spec())
        else:
            log.debug('Skipping.')
            return True

    @status_component('URL Availability', basic_status)
    def urls_ready(self):
        urls = self.get_kwargs().get('urls', list())
        for fmt in urls:
            url = self.resolve_url(fmt)
            log.debug('Checking URL availability: %r', url)
            available = ib.get('synch.site_available', url)
            if not available:
                log.info('Site %r is still not available.', url)
                return False
            else:
                log.info('Site %r has become available.', url)

        return True

    @status_component('Attribute Availability', basic_status)
    def attributes_ready(self):
        """
        .. todo:: Make this more flexible (check for specific values, match
            regex, etc.)
        """
        synch_attrs = self.resolved_node_definition.get('synch_attrs')
        if not synch_attrs:
            # Nothing to synchronize upon
            return True

        node_id = self.node_id

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
