#
# Copyright (C) 2014 MTA SZTAKI
#

""" Resolution of node definitions.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This module provides utilities to resolve the definition of an infrastructure
node. That is, it uses the abstract definition of the node's type (its
:ref:`implementation <nodedefinition>`) and gathers all the information
necessary to actually create and build such a node on a given backend.

The correct resolution algorithm **depends on** the type of description, which
is a function of the type of the **service composer** and the type of the
**cloud handler** used to instantiate the node. Because of this dependency,
the resolution utilises the :mod:`Factory <occo.util.factory>` pattern to
select the correct :class:`Resolver`.

.. autoclass:: ChefCloudinitResolver
    :members:
"""

__all__ = ['resolve_node', 'Resolver']

import logging
import occo.util as util
import occo.util.factory as factory
import yaml
import jinja2

log = logging.getLogger('occo.infraprocessor.node_resolution')

def resolve_node(ib, node_id, node_description):
    """
    Resolve node definition

    :param str node_id: The internal unique identifier of the node.
    :param node_description: The description of the node type. This will be
        resolved to :ref:`Node Definition <nodedefinition>`, which will then
        be resolved, filled in with information, to get a
        :ref:`Resolved Node Definition <resolvednode>`.
    :type node_description: :ref:`Node Description <nodedescription>`

    .. todo:: Alternative, future version:

        .. code:: python

            node_definition = brokering_service.select_implementation(
                                           node['type'], node.get('backend_id'))
            # The brokering service will call ib.get() as necessary.
    """
    node_definition = ib.get('node.definition',
                             node_description['type'],
                             node_description.get('backend_id'))
    resolver = factory.Resolver.instantiate(
        node_definition['implementation_type'],
        info_broker=ib,
        node_id=node_id,
        node_description=node_description)
    resolver.resolve_node(node_definition)
    return node_definition

class Resolver(factory.MultiBackend):
    """
    Abstract interface for node resolution.

    This is an :class:`abstract factory <occo.util.factory.factory>` class.

    :param protocol: The key identifying the actual resolver class.

    :param info_broker: A reference to an information provider.
    :type info_broker: :class:`~occo.infobroker.provider.InfoProvider`

    :param str node_id: The internal identifier of the node instance.

    :param node_description: The original node description.
    :type node_description: :ref:`Node Description <nodedescription>`
    """
    def __init__(self, info_broker, node_id, node_description):
        self.info_broker = info_broker
        self.node_id = node_id
        self.node_description = node_description
    def resolve_node(self, node_definition):
        """
        Overriden in a sub-class, ``node_definition`` should be updated
        in-place with the necessary information. ``node_definition`` is also
        returned for convenience. The resulting :ref:`resolved node definition
        <resolvednode>` must contain all information intended for both the
        service composer and the cloud handler.

        :param node_definition: The definition that needs to be resolved.
            The definition will be updated in place.
        :type node_definition: :ref:`Node Definition <nodedefinition>`

        :return: A reference to ``node_definition``.
        :rtype: :ref:`Resolved Node Definition <resolvednode>`
        """
        return node_definition

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
                'node.find', node['environment_id'], node_name)
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

@factory.register(Resolver, 'cooked')
class IdentityResolver(Resolver):
    """
    Implementation of :class:`Resolver` for implementations already resolved.

    This resolver returns the node_definition as-is.
    """
    def resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """
        pass
