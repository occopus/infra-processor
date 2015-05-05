#
# Copyright (C) 2014 MTA SZTAKI
#

""" Chef resolver for node definitions.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__ = ['ChefCloudinitResolver']

import logging
import occo.util as util
import occo.util.factory as factory
import yaml
import jinja2
from ...node_resolution import Resolver

log = logging.getLogger('occo.infraprocessor.node_resolution.chef')

@factory.register(Resolver, 'chef+cloudinit')
class ChefCloudinitResolver(Resolver):
    """
    Implementation of :class:`Resolver` for implementations for `cloud-init`_
    with Chef_.

    This resolver updates the node definition with infromation required by the
    Chef :ref:`Service Composer <servicecomposer>` and any kind of :ref:`Cloud
    Handler <cloudhandler>` that uses cloud-init; for example the
    :class:`~occo.cloudhandler.backend.boto.BotoCloudHandler`.

    .. todo:: This aspect of OCCO (resolving) must be thought over.

    .. _`cloud-init`: https://cloudinit.readthedocs.org/en/latest/
    .. _Chef: https://www.chef.io/
    """

    def extract_template(self, node_definition):

        def context_list():
            # `context_template` is also removed from the definition, as
            # it will be replaced with the rendered `context`
            yield ('node definition',
                   node_definition.pop('context_template', None))

            from occo.infobroker import main_info_broker
            sc_data = main_info_broker.get(
                'service_composer.aux_data',
                node_definition['service_composer_id'])
            yield ('service composer default',
                   sc_data.get('context_template', None))

            yield ('n/a', '')

        context_source, context_template = next(
            i for i in context_list() if i[1] is not None)
        log.debug('Context template from %s:\n%s',
                  context_source, context_template)

        return jinja2.Template(context_template)

    def attr_template_resolve(self, attrs, template_data):
        if type(attrs) is dict:
            for k, v in attrs.iteritems():
                attrs[k] = self.attr_template_resolve(v, template_data)
            return attrs
        elif type(attrs) is list:
            for i in xrange(len(attrs)):
                attrs[i] = self.attr_template_resolve(attrs[i], template_data)
            return attrs
        elif type(attrs) is str:
            template = jinja2.Template(attrs)
            return template.render(**template_data)
        else:
            return attrs

    def attr_connect_resolve(self, node, attrs, attr_mapping):
        connections = [
            dict(source_role="{0}_{1}".format(node['environment_id'], role),
                 source_attribute=mapping['attributes'][0],
                 destination_attribute=mapping['attributes'][1])
            for role, mappings in attr_mapping.iteritems()
            for mapping in mappings
        ]

        attrs['connections'] = connections

    def resolve_attributes(self, node, node_definition, template_data):
        attrs = node_definition.get('attributes', dict())
        attrs.update(node.get('attributes', dict()))
        attr_mapping = node.get('mappings', dict()).get('inbound', dict())

        self.attr_template_resolve(attrs, template_data)
        self.attr_connect_resolve(node, attrs, attr_mapping)

        return attrs

    def extract_synch_attrs(self, node):
        """
        Fill synch_attrs.

        .. todo:: Maybe this should be moved to the Compiler. The IP
            depends on it, not the Chef service-composer.
        """
        outedges = node.get('mappings', dict()).get('outbound', dict())

        return [mapping['attributes'][0]
                for mappings in outedges.itervalues() for mapping in mappings
                if mapping['synch']]

    def assemble_template_data(self, node, node_definition):
        from occo.infobroker import main_info_broker

        def find_node_id(node_name):
            nodes = main_info_broker.get(
                'node.find', infra_id=node['environment_id'], name=node_name)
            if not nodes:
                raise KeyError(
                    'No node exists with the given name', node_name)
            elif len(nodes) > 1:
                log.warning(
                    'There are multiple nodes with the same node name. '
                    'Choosing the first one as default (%s)',
                    nodes[0]['node_id'])
            return nodes[0]

        source_data = dict(node_id=self.node_id)
        source_data.update(node)
        source_data.update(node_definition)
        source_data['ibget'] = main_info_broker.get
        source_data['find_node_id'] = find_node_id
        return source_data

    def render_template(self, node, node_definition, template_data):
        template = self.extract_template(node_definition)
        return template.render(**template_data)

    def resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """

        # Shorthands
        node = self.node_description
        ib = self.info_broker
        node_id = self.node_id
        template_data = self.assemble_template_data(node, node_definition)

        # Amend resolved node with new information
        node_definition['node_id'] = node_id
        node_definition['name'] = node['name']
        node_definition['environment_id'] = node['environment_id']
        node_definition['auth_data'] = ib.get('backends.auth_data',
                                              node_definition['backend_id'],
                                              node['user_id'])
        node_definition['context'] = self.render_template(
            node, node_definition, template_data)
        node_definition['attributes'] = self.resolve_attributes(
            node, node_definition, template_data)
        node_definition['synch_attrs'] = \
            self.extract_synch_attrs(node)
