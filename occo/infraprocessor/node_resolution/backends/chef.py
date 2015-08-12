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
import occo.exceptions as exceptions
import occo.util.factory as factory
import yaml
import jinja2
from ..resolution import Resolver

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
            yield ('node_definition',
                   node_definition.pop('context_template', None))

            from occo.infobroker import main_info_broker
            sc_data = main_info_broker.get(
                'service_composer.aux_data',
                node_definition['service_composer_id'])
            yield ('service_composer_default',
                   sc_data.get('context_template', None))

            yield 'default', ''

        src, template = util.find_effective_setting(context_list())
        log.debug('Context template from %s:\n%s', src, template)

        return jinja2.Template(template)

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
            dict(source_role="{0}_{1}".format(node['infra_id'], role),
                 source_attribute=mapping['attributes'][0],
                 destination_attribute=mapping['attributes'][1])
            for role, mappings in attr_mapping.iteritems()
            for mapping in mappings
        ]

        attrs['connections'] = connections

    def resolve_attributes(self, node_desc, node_definition, template_data):
        attrs = node_definition.get('attributes', dict())
        attrs.update(node_desc.get('attributes', dict()))
        attr_mapping = node_desc.get('mappings', dict()).get('inbound', dict())

        self.attr_template_resolve(attrs, template_data)
        self.attr_connect_resolve(node_desc, attrs, attr_mapping)

        return attrs

    def extract_synch_attrs(self, node_desc):
        """
        Fill synch_attrs.

        .. todo:: Maybe this should be moved to the Compiler. The IP
            depends on it, not the Chef service-composer.
        """
        outedges = node_desc.get('mappings', dict()).get('outbound', dict())

        return [mapping['attributes'][0]
                for mappings in outedges.itervalues() for mapping in mappings
                if mapping['synch']]

    def assemble_template_data(self, node_desc, node_definition):
        from occo.infobroker import main_info_broker

        def find_node_id(node_name):
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

        # As long as source_data is read-only, the following code is fine.
        # As it is used only for rendering a template, it is yet read-only.
        # If, for any reason, something starts modifying it, dict.update()-s
        # will have to be changed to deep copy to avoid side effects. (Just
        # like in compiler, when node variables are assembled.)
        source_data = dict(node_id=self.node_id)
        source_data.update(node_desc)
        source_data.update(node_definition)
        source_data['ibget'] = main_info_broker.get
        source_data['find_node_id'] = find_node_id
        return source_data

    def check_if_cloud_config(self, node_definition):
        if not node_definition['context'].startswith('#cloud-config'):
            return
        try:
            yaml.load(node_definition['context'])
        except yaml.YAMLError as e:
            if hasattr(e, 'problem_mark'):
                msg=('Schema error in context of '
                     'node definition at line {0}.').format( e.problem_mark.line)
            else:
                msg='Schema error in context of node definition.'
            raise exceptions.NodeContextSchemaError(
                    node_definition=node_definition, reason=e, msg=msg)

    def check_template(self, node_definition):
        """Checks the context part of node definition"""
        self.check_if_cloud_config(node_definition)
        pass

    def render_template(self, node_definition, template_data):
        """Renders the template pertaining to the node definition"""
        template = self.extract_template(node_definition)
        return template.render(**template_data)

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
        node_definition['node_id'] = node_id
        node_definition['name'] = node_desc['name']
        node_definition['infra_id'] = node_desc['infra_id']
        node_definition['auth_data'] = ib.get('backends.auth_data',
                                              node_definition['backend_id'],
                                              node_desc['user_id'])
        node_definition['context'] = self.render_template(node_definition,
                                                          template_data)
        node_definition['attributes'] = self.resolve_attributes(
            node_desc, node_definition, template_data)
        node_definition['synch_attrs'] = \
            self.extract_synch_attrs(node_desc)

        # Check context
        self.check_template(node_definition)
