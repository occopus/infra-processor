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

__all__ = ['SynchronizationProvider', 'CompositeStatus', 'status_component']

import logging
import occo.util as util
import occo.infobroker as ib

log = logging.getLogger('occo.infraprocessor.synchronization')

def format_bool(b):
    return 'OK' if b else 'PENDING'

class status_component(object):
    """ Decorator to gather status components. """
    def __init__(self, description, *aggregators):
        self.aggregators, self.desc = aggregators, description
    def __call__(self, fun):
        for i in self.aggregators:
            i.add_component(self.desc, fun)
        return fun

class CompositeStatus(object):
    """Represents a composite status. TODO: rewrite to not use external lists"""
    def __init__(self, *items):
        self.items = list(items)
    def add_component(self, desc, fun):
        self.items.append(dict(desc=desc, fun=fun))
    def get_status(self, lazy=True, *args, **kwargs):
        status = True
        for item in self.items:
            desc, fun = item['desc'], item['fun']
            log.debug('    Querying %r (%r)...', desc, kwargs)
            val = fun(self, *args, **kwargs)
            log.info('    %s => %s', desc, format_bool(val))
            status = status and val
            if lazy and not status:
                log.debug('    Lazy evaluation: skipping remaining items.')
                break
        log.info('Status: %s', format_bool(status))
        return status

@ib.provider
class SynchronizationProvider(ib.InfoProvider):
    @ib.provides('node.address')
    def get_server_address(self, **node_spec):
        inst = ib.main_info_broker.get('node.find_one', **node_spec)
        return ib.main_info_broker.get(
            'node.cloud_attribute', 'ipaddress', inst)

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
        response = util.do_request(url, 'head', **kwargs)
        return response.success
