#
# Copyright (C) 2014 MTA SZTAKI
#

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

log = logging.getLogger('occo.infraprocessor.synchronization')

def format_bool(b):
    return 'OK' if b else 'PENDING'

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

        status = all(results)
        log.info('Status of %r: %s', tag.name, format_bool(status))
        return status

    def get_detailed_status(self, tag, *args, **kwargs):
        log.debug('Evaluating status of %r', tag.name)
        return list(item.evaluate(self, *args, **kwargs) for item in tag.items)


@ib.provider
class SynchronizationProvider(ib.InfoProvider):
    @ib.provides('node.address')
    def get_server_address(self, **node_spec):
        inst = ib.main_info_broker.get('node.find_one', **node_spec)
        return ib.main_info_broker.get('node.resource.address', inst)

    @ib.provides('synch.node_reachable')
    @ib.provides('node.network_reachable')
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
    def site_available(self, url, **kwargs):
        try:
            response = util.do_request(url, 'head', **kwargs)
        except (util.HTTPTimeout, util.HTTPError) as ex:
            log.warning('Error accessing [%s]: %s', url, ex)
            return False
        else:
            return response.success
