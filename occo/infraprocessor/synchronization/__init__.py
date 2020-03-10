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
from occo.exceptions import SchemaError

log = logging.getLogger('occo.infraprocessor.synchronization')
ib = occo.infobroker.main_info_broker

PROTOCOL_ID = 'basic'

def format_bool(b):
    return node_status.READY if b else node_status.PENDING

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
    synchstrat = resolved_node_definition.get('health_check')
    if synchstrat:
        # The synch strategy may be parameterizable (like 'basic' is)
        key = synchstrat.get('protocol','basic') \
            if isinstance(synchstrat, dict) \
            else synchstrat
        if not NodeSynchStrategy.has_backend(key):
            # If specified, but unknown, that is an error (typo or misconfig.)
            raise ValueError('Unknown health_check', key)
    else:
        # No special synch strategy has been defined.
        key = 'basic'

    log.debug('Health checking protocol is %r', key)
    return key

def get_synch_strategy(instance_data):
    node_description = instance_data['node_description']
    resolved_node_definition = instance_data['resolved_node_definition']
    synch_type = node_synch_type(resolved_node_definition)
    log.debug('Health checking strategy for node %r/%r is %r.',
             node_description['name'], instance_data['node_id'], synch_type)
    log.info('Health checking for node %r/%r',
             node_description['name'], instance_data['node_id'])

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

    node_id = instance_data['node_id']
    node_name = instance_data.get('resolved_node_definition',dict()).get('name',"undefined")

    if timeout:
        start_time = time.time()
        finish_time = start_time + timeout
        log.info(('Waiting for node %r/%r to become ready with '
                  '%d seconds timeout. Deadline: %s'),
                 node_name,
                 node_id,
                 timeout,
                 datetime.datetime.fromtimestamp(finish_time).isoformat())
    else:
        log.info('Waiting for node %r/%r to become ready. No timeout.', 
            node_name, node_id)

    status = ib.get('node.state', instance_data)
    while status != node_status.READY:
        if timeout and time.time() > finish_time:
            raise NodeCreationTimeOutError(
                    instance_data=instance_data,
                    reason=None,
                    msg=('Timeout ({0}s) in node creation!'
                         .format(timeout)))

        if status in [node_status.SHUTDOWN, node_status.FAIL]:
            raise NodeFailedError(instance_data, status)

        log.debug('Node %r/%r is not ready, waiting %r seconds.',
                  node_name, node_id, poll_delay)
        if not sleep(poll_delay, cancel_event):
            log.debug('Waiting for node %r/%r has been cancelled.', node_name, node_id)
            return
        status = ib.get('node.state', instance_data)

    log.info('Node %r/%r is ready.', node_name, node_id)

class NodeSynchStrategy(factory.MultiBackend):
    """
    Abstract strategy to check whether a node is ready to be used.

    :param node_description: The node description as provided by the Compiler.
        It is the same as the input of the InfraProcessor's CreateNode command.

    :param resolved_node_definition: The node definition as resolved by the
        InfraProcessor. It is the same as the input of the ResourceHandler and
        ConfigManager's CreateNode commands.

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
        self.node_address = ib.get('node.address', infra_id=self.infra_id, node_id=self.node_id)

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

@factory.register(NodeSynchStrategy, PROTOCOL_ID)
class BasicNodeSynchStrategy(CompositeStatus, NodeSynchStrategy):
    """
    Default synchronization strategy. This strategy ensures the following
    properties of the node:
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

    def get_kwargs(self):
        """
        .. todo:: Make this more generic (not only BasicNodeSynchStrategy will
            be parameterizable.
        """
        if not hasattr(self, 'kwargs'):
            self.kwargs = self.resolved_node_definition.get(
                'health_check', dict())
            if isinstance(self.kwargs, str):
                # health_check has been specified as a non-parameterized
                # string.
                self.kwargs = dict()
        return self.kwargs

    def make_node_spec(self):
        return dict(infra_id=self.infra_id, node_id=self.node_id)

    def get_node_address(self):
        return self.node_address

    def resolve_parameter(self, fmt):
        data = dict(
            node_id=self.instance_data['node_id'],
            ibget=ib.get,
            instance_data=self.instance_data,
            variables=self.node_description.get('variables'),
            ip=self.get_node_address(),
        )
        import jinja2
        tmp = jinja2.Template(fmt)
        return tmp.render(data)

    @status_component('Network reachability', basic_status)
    def reachable(self):
        host = self.get_node_address()
        if self.get_kwargs().get('ping', True):
            log.info('  Checking node reachability (%s):', self.node_id)
            result = ib.get('synch.node_reachable', host)
            log.info('    %s => %s', host, format_bool(result))
            return result
        else:
            return True

    @status_component('Port Availability', basic_status)
    def ports_ready(self):
        host = self.get_node_address()
        ports = self.get_kwargs().get('ports', list())
        if ports:
            result = True
            log.info('  Checking port availability (%s):', self.node_id)
            for port in ports:
                available = ib.get('synch.port_available', host, port)
                log.info('    %s => %s', port, format_bool(available))
                if not available:
                    result = False
            return result
        return True 

    @status_component('URL Availability', basic_status)
    def urls_ready(self):
        urls = self.get_kwargs().get('urls', list())
        if urls:
            result = True
            log.info('  Checking url availability (%s):', self.node_id)
            for fmt in urls:
                url = self.resolve_parameter(fmt)
                available = ib.get('synch.site_available', url)
                log.info('    %s => %s', url, format_bool(available))
                if not available:
                    result = False
            return result
        return True

    @status_component('Attribute Availability', basic_status)
    def attributes_ready(self):
        """
        .. todo:: Make this more flexible (check for specific values, match
            regex, etc.)
        """
        synch_attrs = self.resolved_node_definition.get('synch_attrs')
        if not synch_attrs:
            return True
        result = True
        log.info('  Checking attribute availability (%s):', self.node_id)
        for attribute in synch_attrs:
            try:
                value = ib.get('node.attribute', self.node_id, attribute)
                log.info('    %s => %s', attribute, format_bool(True))
            except KeyError:
                log.info('    %s => %s', attribute, format_bool(False))
                result = False
                continue
            if value is None:
                log.info('    %s => %s', attribute, format_bool(False))
                result = False
        return result

    @status_component('Mysql database availability', basic_status)
    def mysqldbs_ready(self):
        host = self.get_node_address()
        dblist = self.get_kwargs().get('mysqldbs', list())    
        if len(dblist):
            result = True
            log.info('  Checking mysql availability (%s):', self.node_id)
            for db in dblist:
                name = self.resolve_parameter(db.get('name'))
                user = self.resolve_parameter(db.get('user'))
                pwd = self.resolve_parameter(db.get('pass')) 
                available = ib.get('synch.mysql_ready', host, name, user, pwd)
                log.info('    %s/%s => %s', name, user, format_bool(available))
                if not available:
                    result = False
            return result
        return True


class HCSchemaChecker(factory.MultiBackend):
    def __init__(self):
        return

    def perform_check(self, data):
        raise NotImplementedError()

    def get_missing_keys(self, data, req_keys):
        missing_keys = list()
        for rkey in req_keys:
            if rkey not in data:
                missing_keys.append(rkey)
        return missing_keys

    def get_invalid_keys(self, data, valid_keys):
        invalid_keys = list()
        for key in data:
            if key not in valid_keys:
                invalid_keys.append(key)
        return invalid_keys

@factory.register(HCSchemaChecker, PROTOCOL_ID)
class BasicHCSchemaChecker(HCSchemaChecker):
    def __init__(self):
#        super(__init__(), self)
        self.req_keys = []
        self.opt_keys = ['type', 'mysqldbs', 'ports', 'urls', 'ping', 'timeout']
    def perform_check(self, data):
        missing_keys = HCSchemaChecker.get_missing_keys(self, data, self.req_keys)
        if missing_keys:
            msg = "Missing key(s): " + ', '.join(str(key) for key in missing_keys)
            raise SchemaError(msg)
        valid_keys = self.req_keys + self.opt_keys
        invalid_keys = HCSchemaChecker.get_invalid_keys(self, data, valid_keys)
        if invalid_keys:
            msg = "Unknown key(s): " + ', '.join(str(key) for key in invalid_keys)
            raise SchemaError(msg)
        if 'mysqldbs' in data:
            if type(data['mysqldbs']) is list:
                keys = ['name', 'user', 'pass']
                for db in data['mysqldbs']:
                    mkeys = HCSchemaChecker.get_missing_keys(self, db, keys)
                    ikeys = HCSchemaChecker.get_invalid_keys(self, db, keys)
                if mkeys:
                    msg = "Missing key(s) in mysqldbs: " +  ', '.join(str(key) for key in mkeys)
                    raise SchemaError(msg)
                if ikeys:
                    msg = "Unknown key(s) in mysqldbs: " +  ', '.join(str(key) for key in mkeys)
                    raise SchemaError(msg)
            else:
                raise SchemaError("Invalid format of \'mysqldbs\' section! Must be a list.")
        if 'urls' in data:
            if type(data['urls']) is not list:
                raise SchemaError("Invalid format of \'urls\' section! Must be a list.")
        if 'ports' in data:
            if type(data['ports']) is list:
                for port in data['ports']:
                    if not isinstance(port, int):
                        msg = "Invalid port %r - must be integer" % (port)
                        raise SchemaError(msg)
            else:
                raise SchemaError("Invalid format of \'ports\' section! Must be a list.")
        if 'ping' in data:
            if not isinstance(data['ping'], bool):
                 raise SchemaError("Invalid value of \'ping\' section! Must be True/False.")
        if 'timeout' in data:
            if not isinstance(data['timeout'], int):
                 raise SchemaError("Invalid value of \'timeout\' section! Must be integer.")
        return True

