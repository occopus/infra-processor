#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['resolve_node']

import logging
import occo.util as util
import occo.util.factory as factory
import yaml

log = logging.getLogger('occo.infraprocessor.node_resolution')

def resolve_node(ib, node_id, node_description):
    # Resolve node definition
        # TODO: Alternative, future version:
        # node_definition = brokering_service.select_implementation(
        #                                node['type'], node.get('backend_id'))
        # The brokering servie will call ib.get() as necessary.
    node_definition = ib.get('node.definition',
                             node_description['type'],
                             node_description.get('backend_id'))
    resolver = Resolver(protocol=node_definition['implementation_type'],
                        info_broker=ib,
                        node_id=node_id,
                        node_description=node_description)
    resolver.resolve_node(node_definition)
    return node_definition

class Resolver(factory.MultiBackend):
    def __init__(self, protocol, info_broker, node_id, node_description):
        self.info_broker = info_broker
        self.node_id = node_id
        self.node_description = node_description
    def resolve_node(self, node_definition):
        """
        In a sub-class, node_definition should be updated in-place with
        the necessary information.
        node_definition is also returned for convenience.
        The resulting node definition must contain all information intended for
        both the service composer and the cloud handler.
        """
        return node_definition

@factory.register(Resolver, 'chef+cloudinit')
class ChefCloudinitResolver(Resolver):
    def resolve_node(self, node_definition):
        # Shorthands
        node = self.node_description
        ib = self.info_broker
        node_id = self.node_id

        # Amend resolved node with basic information
        node_definition['id'] = node_id
        node_definition['name'] = node['name']
        node_definition['environment_id'] = node['environment_id']
        # Resolve backend-specific authentication information
        node_definition['auth_data'] = ib.get('backends.auth_data',
                                              node_definition['backend_id'],
                                              node['user_id'])
        return node_definition
