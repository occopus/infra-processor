#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['resolve_node']

import logging
import occo.util as util
import occo.util.factory as factory

log = logging.getLogger('occo.infraprocessor')

def resolve_node(ib, node_id, node_description):
    resolver = Resolver(node_description['implementation_type'],
                        ib, node_id, node_description)
    return resolver.resolve_node()

class Resolver(factory.MultiBackend):
    def __init__(self, protocol, info_broker, node_id, node_description):
        self.info_broker = info_broker
        self.node_id = node_id
        self.node_description = node_description
    def resolve_node(self):
        raise NotImplementedError()

@factory.register(Resolver, 'chef+cloudinit')
class ChefCloudinitResolver(Resolver):
    def resolve_node(self):
        # Shorthands
        node = self.node_description
        ib = self.info_broker
        node_id = self.node_id

        # Resolve node definition
        resolved_node = ib.get('node.definition',
                               node['type'], node.get('backend_id'))
        # TODO: Alternative, future version:
        # resolved_node = brokering_service.select_implementation(
        #                                node['type'], node.get('backend_id'))
        # The brokering servie will call ib.get() as necessary.

        # Amend resolved node with basic information
        resolved_node['id'] = node_id
        resolved_node['name'] = node['name']
        resolved_node['environment_id'] = node['environment_id']
        # Resolve backend-specific authentication information
        resolved_node['auth_data'] = ib.get('backends.auth_data',
                                            resolved_node['backend_id'],
                                            node['user_id'])
