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

""" Generic synchronization primitives

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

Can be used by implementations of
:class:`~occo.infraprocessor.synchronization.NodeSynchStrategy`
to implement higher level synchronizations.

These primitives are exposed through a the InfoBroker system.
"""

from __future__ import absolute_import

__all__ = ['SynchronizationProvider', 'CompositeStatus', 'status_component',
           'StatusTag']

import logging
import occo.util as util
import occo.infobroker as ib
from occo.exceptions import ConnectionError, HTTPTimeout, HTTPError
import occo.constants.status as node_status

log = logging.getLogger('occo.infraprocessor.synchronization')

DUMMY_REPORT = dict(
    ready=True,
    details={},
    DUMMY_REPORT=True,
)

def format_bool(b):
    return 'READY' if b else 'PENDING'

class StatusItem(object):
    def __init__(self, description, fun):
        self.desc, self.fun = description, fun

    def evaluate(self, fun_self, *args, **kwargs):
        log.debug('    Querying %r (%r, %r)...',
                  self.desc, args, kwargs)
        val = self.fun(fun_self, *args, **kwargs)
        log.info('    %s => %s', self.desc, format_bool(val))
        return val

class StatusTag(object):
    """ Status components can be gathered in a tag object. """
    def __init__(self, name):
        self.items, self.name = list(), name
    def add_component(self, desc, fun):
        self.items.append(StatusItem(desc, fun))

class status_component(object):
    """ Decorator to gather status components. """
    def __init__(self, description, *tags):
        self.tags, self.desc = tags, description
    def __call__(self, fun):
        for i in self.tags:
            i.add_component(self.desc, fun)
        return fun

class CompositeStatus(object):
    """Represents a composite status. """
    def get_composite_status(self, tag, lazy=True, *args, **kwargs):
        log.debug('Evaluating status of %r', tag.name)

        results = (item.evaluate(self, *args, **kwargs) for item in tag.items)
        if not lazy:
            # list() force-evaluates all items
            results = list(results)
        # all() is lazy; if force-evaluation is omitted, evaluation will stop
        # at the first False
        status = all(results)
        log.info('Status of %r: %s', tag.name, format_bool(status))
        return status

    def get_detailed_status(self, tag, *args, **kwargs):
        log.debug('Evaluating status of %r', tag.name)
        return list(item.evaluate(self, *args, **kwargs) for item in tag.items)

    def get_report(self, tag, *args, **kwargs):
        log.debug('Evaluating status of %r', tag.name)
        return list((item.desc,
                     item.evaluate(self, *args, **kwargs))
                    for item in tag.items)

@ib.provider
class SynchronizationProvider(ib.InfoProvider):
    @ib.provides('node.address')
    @util.wet_method('127.0.0.1')
    def get_server_address(self, **node_spec):
        inst = ib.main_info_broker.get('node.find_one', **node_spec)
        return ib.main_info_broker.get('node.resource.address', inst)

    @ib.provides('synch.node_reachable')
    @ib.provides('node.network_reachable')
    @util.wet_method(True)
    def reachable(self, **node_spec):
        addr = ib.main_info_broker.get('node.address', **node_spec)
        try:
            retval, out, err = \
                util.basic_run_process(
                    'ping -c 1 -W 1 {addr}'.format(addr=addr))
        except Exception:
            log.exception('Process execution failed:')
            raise
        else:
            log.debug('Process STDOUT:\n%s', out)
            log.debug('Process STDERR:\n%s', err)
            log.debug('Process exit code: %d', retval)
            return (retval == 0)

    @ib.provides('synch.site_available')
    @util.wet_method(True)
    def site_available(self, url, **kwargs):
        try:
            log.debug('Checking site availability: %r', url)
            response = util.do_request(url, 'head', **kwargs)
        except (ConnectionError, HTTPTimeout, HTTPError) as ex:
            log.warning('Error accessing [%s]: %s', url, ex)
            return False
        else:
            return response.success

    @ib.provides('synch.mysql_ready')
    @util.wet_method(True)
    def mysql_ready(self, host, dbname, dbuser, dbpass):
        import MySQLdb
        try:
            log.debug('Checking mysqldb name: %s, user: %s, pass: %s',dbname,dbuser,dbpass)
            conn = MySQLdb.connect(host, dbuser, dbpass, dbname)
            conn.close()
            log.debug('Connection successful')
        except MySQLdb.Error as e:
            log.debug('Connecton failed: %s',e)
            return False
        return True

    @ib.provides('node.state_report')
    @util.wet_method(DUMMY_REPORT)
    def node_state_report(self, instance_data):
        log.debug('Acquiring detailed node status report')
        from ..synchronization import get_synch_strategy
        strategy = get_synch_strategy(instance_data)
        report = strategy.generate_report()
        return dict(
            ready=all(r[1] for r in report),
            details=report)

    def _get_instance_reports(self, instances):
        return util.dict_map(instances, self.node_state_report)

    @ib.provides('infrastructure.state_report')
    @util.wet_method(DUMMY_REPORT)
    def infra_state_report(self, infra_id):
        log.debug('Acquiring detailed infrastructure status report')
        dynamic_state = \
            ib.main_info_broker.get('infrastructure.state', infra_id)

        details = util.dict_map(dynamic_state, self._get_instance_reports)
        ready = all(i['ready']
                    for j in details.itervalues()
                    for i in j.itervalues())
        return dict(details=details, ready=ready)

    @ib.provides('node.service_health_check.state')
    @util.wet_method('READY')
    def service_verification_state(self, instance_data):
        log.debug('Acquiring service health check state')
        from ..synchronization import get_synch_strategy
        strategy = get_synch_strategy(instance_data)
        state = strategy.is_ready()
        return node_status.READY if state else node_status.PENDING


