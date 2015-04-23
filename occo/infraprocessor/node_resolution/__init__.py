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
"""

__import__('pkg_resources').declare_namespace(__name__)

__all__ = ['resolve_node', 'Resolver']

import logging
import occo.util.factory as factory

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
    resolver = Resolver.instantiate(
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

@factory.register(Resolver, 'cooked')
class IdentityResolver(Resolver):
    """
    Implementation of :class:`Resolver` for definitions already resolved.

    This resolver returns the node_definition as-is.
    """
    def resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """
        pass
