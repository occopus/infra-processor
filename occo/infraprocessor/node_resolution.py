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

""" Resolution of node definitions.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This module provides utilities to resolve the definition of an infrastructure
node. That is, it uses the abstract definition of the node's type (its
:ref:`implementation <nodedefinition>`) and gathers all the information
necessary to actually create and build such a node on a given backend.

The correct resolution algorithm **depends on** the type of description, which
is a function of the type of the **service composer** and the type of the
**resource handler** used to instantiate the node. Because of this dependency,
the resolution utilises the :mod:`Factory <occo.util.factory>` pattern to
select the correct :class:`Resolver`.
"""


__all__ = ['resolve_node', 'Resolver']

import logging
import occo.util as util
import occo.util.factory as factory

log = logging.getLogger('occo.infraprocessor.node_resolution')

def resolve_node(ib, node_id, node_description, default_timeout=None):
    """
    Resolve node description

    :param str node_id: The internal unique identifier of the node.
    :param node_description: The description of the node type. This will be
        resolved to :ref:`Node Definition <nodedefinition>`, which will then
        be resolved, filled in with information, to get a
        :ref:`Resolved Node Definition <resolvednode>`.
    :type node_description: :ref:`Node Description <nodedescription>`
    """
    node_definition = ib.get(
        'node.definition',
        node_description['type'],
        filter_keywords = node_description.get('filter'),
        strategy=node_description.get('backend_selection_strategy', 'random'))

    resolver = Resolver.instantiate(
        protocol=node_definition.get('contextualisation',dict()).get('type','basic'),
        info_broker=ib,
        node_id=node_id,
        node_description=node_description,
        default_timeout=default_timeout
    )
    log.debug('Resolving node using %r', resolver.__class__)

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
    def __init__(self, info_broker, node_id, node_description,
                 default_timeout=None):
        self.info_broker = info_broker
        self.node_id = node_id
        self.node_description = node_description
        self.default_timeout = default_timeout

    def determine_timeout(self, node_definition):
        def possible_timeouts():
            yield 'node_desc', self.node_description.get('create_timeout')
            yield 'node_def', node_definition.get('create_timeout')
            yield 'config_default', self.default_timeout

        src, timeout = util.find_effective_setting(possible_timeouts(), True)

        log.debug('Effective timeout is %r (from %s)', timeout, src)
        return timeout

    def resolve_node(self, node_definition):
        """
        Resolve the node definition using :meth:`_resolve_node` and then amend
        it with universally required information (e.g. creation timeout)
        """
        self._resolve_node(node_definition)
        node_definition['create_timeout'] = \
            self.determine_timeout(node_definition)

    def _resolve_node(self, node_definition):
        """
        Overriden in a sub-class, ``node_definition`` should be updated
        in-place with the necessary information. ``node_definition`` is also
        returned for convenience. The resulting :ref:`resolved node definition
        <resolvednode>` must contain all information intended for both the
        service composer and the resource handler.

        :param node_definition: The definition that needs to be resolved.
            The definition will be updated in place.
        :type node_definition: :ref:`Node Definition <nodedefinition>`

        :return: A reference to ``node_definition``.
        :rtype: :ref:`Resolved Node Definition <resolvednode>`
        """
        raise NotImplementedError()

@factory.register(Resolver, 'cooked')
class IdentityResolver(Resolver):
    """
    Implementation of :class:`Resolver` for definitions already resolved.

    This resolver returns the node_definition as-is.
    """
    def _resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """
        desc = self.node_description
        node_definition['node_id'] = self.node_id
        node_definition['infra_id'] = desc['infra_id']
        node_definition['user_id'] = desc['user_id']

class ContextSchemaChecker(factory.MultiBackend):
    def __init__(self):
        return

    def perform_check(self, data):
        raise NotImplementedError()

    def get_missing_keys(self, data, req_keys):
        missing_keys = list()
        for rkey in req_keys:
            if rkey not in data:
                missing_keys.append(rkey)
        return missing_keys

    def get_invalid_keys(self, data, valid_keys):
        invalid_keys = list()
        for key in data:
            if key not in valid_keys:
                invalid_keys.append(key)
        return invalid_keys

