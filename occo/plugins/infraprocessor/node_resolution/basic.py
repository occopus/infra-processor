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
""" Basic resolver for node definitions.

.. moduleauthor:: Jozsef Kovacs <jozsef.kovacs@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__ = ['BasicResolver']

import logging
import occo.util as util
import occo.exceptions as exceptions
import occo.util.factory as factory
import sys
import yaml
import jinja2
from occo.infraprocessor.node_resolution import Resolver

log = logging.getLogger('occo.infraprocessor.node_resolution.basic')

@factory.register(Resolver, 'basic')
class BasicResolver(Resolver):
    """
    Implementation of :class:`Resolver` for performing basic resolution.
    """

    def attr_template_resolve(self, attrs, template_data):
        """
        Recursively render attributes.
        """
        if isinstance(attrs, dict):
            for k, v in attrs.iteritems():
                attrs[k] = self.attr_template_resolve(v, template_data)
            return attrs
        elif isinstance(attrs, list):
            for i in xrange(len(attrs)):
                attrs[i] = self.attr_template_resolve(attrs[i], template_data)
            return attrs
        elif isinstance(attrs, basestring):
            template = jinja2.Template(attrs)
            return template.render(**template_data)
        else:
            return attrs

    def attr_connect_resolve(self, node, attrs, attr_mapping):
        """
        Transform connection specifications into an attribute that the cookbook
        `connect`_ can understand.
        """
        connections = [
            dict(source_role="{0}_{1}".format(node['infra_id'], role),
                 source_attribute=mapping['attributes'][0],
                 destination_attribute=mapping['attributes'][1])
            for role, mappings in attr_mapping.iteritems()
            for mapping in mappings
        ]

        attrs['connections'] = connections

    def resolve_attributes(self, node_desc, node_definition, template_data):
        """
        Resolve the attributes of a node:
        - Merge attributes of the node desc. and node def. (node desc overrides)
        - Resolve string attributes as Jinja templates
        - Construct an attribute to connect nodes
        """
        attrs = node_definition.get('contextualisation',dict()).get('attributes', dict())
        attrs.update(node_desc.get('attributes', dict()))
        attr_mapping = node_desc.get('mappings', dict()).get('inbound', dict())

        self.attr_template_resolve(attrs, template_data)
        self.attr_connect_resolve(node_desc, attrs, attr_mapping)

        return attrs

    def extract_synch_attrs(self, node_desc):
        """
        Fill synch_attrs.

        .. todo:: Maybe this should be moved to the Compiler. The IP
            depends on it, not the Chef config-manager.

        .. todo:: Furthermore, synch_attrs will be obsoleted, and moved to
            basic health_check as parameters.
        """
        outedges = node_desc.get('mappings', dict()).get('outbound', dict())

        return [mapping['attributes'][0]
                for mappings in outedges.itervalues() for mapping in mappings
                if mapping['synch']]

    def assemble_template_data(self, node_desc, node_definition):
        """
        Create the data structure that can be used in the Jinja templates.

        .. todo:: Document the possibilities.
        """
        from occo.infobroker import main_info_broker

        def find_node_id(node_name):
            """
            Convenience function to be used in templates, to acquire a node id
            based on node name.
            """
            nodes = main_info_broker.get(
                'node.find', infra_id=node_desc['infra_id'], name=node_name)
            if not nodes:
                raise KeyError(
                    'No node exists with the given name', node_name)
            elif len(nodes) > 1:
                log.warning(
                    'There are multiple nodes with the same node name. '
                    'Choosing the first one as default (%s)',
                    nodes[0]['node_id'])
            return nodes[0]

        def getip(node_name):
            return main_info_broker.get('node.resource.address',
                   find_node_id(node_name))

        # As long as source_data is read-only, the following code is fine.
        # As it is used only for rendering a template, it is yet read-only.
        # If, for any reason, something starts modifying it, dict.update()-s
        # will have to be changed to deep copy to avoid side effects. (Just
        # like in Compiler, when node variables are assembled.)
        source_data = dict(node_id=self.node_id)
        source_data.update(node_desc)
        source_data.update(node_definition)
        source_data['ibget'] = main_info_broker.get
        source_data['find_node_id'] = find_node_id
        source_data['getip'] = getip

        #state = main_info_broker.get('node.find', infra_id=node_desc['infra_id'])
        #for node in state:
        #    source_data[node['node_description']['name']] = \
        #    dict(ip=main_info_broker.get('node.resource.address', node))

        return source_data

    def _resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """

        # Shorthands
        node_desc = self.node_description
        ib = self.info_broker
        node_id = self.node_id
        template_data = self.assemble_template_data(node_desc, node_definition)

        # Amend resolved node with new information
        data = {
            'node_id'     : node_id,
            'name'        : node_desc['name'],
            'infra_id'    : node_desc['infra_id'],
            'attributes'  : self.resolve_attributes(node_desc,
                                                    node_definition,
                                                    template_data),
            'synch_attrs' : self.extract_synch_attrs(node_desc),
        }
        node_definition.update(data)
