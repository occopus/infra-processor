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



__all__ = ['DockerResolver']

import base64
import logging
import occo.util as util
import occo.exceptions as exceptions
import occo.util.factory as factory
import sys
from ruamel import yaml
import jinja2
from occo.infraprocessor.node_resolution import Resolver, ContextSchemaChecker
from occo.exceptions import SchemaError

PROTOCOL_ID = 'docker'

log = logging.getLogger('occo.infraprocessor.node_resolution.docker')
datalog = logging.getLogger('occo.data.infraprocessor.node_resolution.docker')

def bencode(value):
    return base64.b64encode(value.encode('utf-8'))

@factory.register(Resolver, PROTOCOL_ID)
class DockerResolver(Resolver):
    """
    Implementation of :class:`Resolver` for performing docker resolution.
    """

    def extract_template(self, node_definition):

        def context_list():
            # `context_variables` is also removed from the definition, as
            # it will be replaced with the rendered `context`
            context_section = node_definition.get('contextualisation', None)
            yield ('node_definition',
                   context_section.pop('context_variables', None))

            yield 'default', ''

        src, template = util.find_effective_setting(context_list())

        if isinstance(template, dict):
              template=yaml.dump(template,default_flow_style=False)

        datalog.debug('Context template from %s:\n%s', src, template)

        return jinja2.Template(template)

    def attr_template_resolve(self, attrs, template_data, context):
        """
        Recursively render attributes.
        """
        if isinstance(attrs, dict):
            for k, v in attrs.items():
                attrs[k] = self.attr_template_resolve(v, template_data, context)
            return attrs
        elif isinstance(attrs, list):
            for i in range(len(attrs)):
                attrs[i] = self.attr_template_resolve(attrs[i], template_data, context)
            return attrs
        elif isinstance(attrs, str):
            loader = jinja2.FileSystemLoader('.')
            env = jinja2.Environment(loader=loader)
            env.filters['b64encode'] = bencode
            template = env.from_string(attrs)
            return template.render(context, **template_data)
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
            for role, mappings in attr_mapping.items()
            for mapping in mappings
        ]

        attrs['connections'] = connections

    def resolve_attributes(self, node_desc, node_definition, template_data, context):
        """
        Resolve the attributes of a node:
        - Merge attributes of the node desc. and node def. (node desc overrides)
        - Resolve string attributes as Jinja templates
        - Construct an attribute to connect nodes
        """
        attrs = node_definition.get('contextualisation',dict()).get('attributes', dict())
        attrs['env'] = node_definition['contextualisation']['env']
        attrs['command'] = node_definition['contextualisation']['command']
        template_data['context_variables'] = {a: context[a] for a in context} if context is not None else {}
        attrs.update(node_desc.get('attributes', dict()))
        attr_mapping = node_desc.get('mappings', dict()).get('inbound', dict())

        self.attr_template_resolve(attrs, template_data, template_data['context_variables'])
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
                for mappings in outedges.values() for mapping in mappings
                if mapping['synch']]

    def assemble_template_data(self, node_desc, node_definition):
        """
        Create the data structure that can be used in the Jinja templates.

        .. todo:: Document the possibilities.
        """
        from occo.infobroker import main_info_broker

        def find_node_id(node_name, allnodes=False):
            """
            Convenience function to be used in templates, to acquire a node id
            based on node name.
            """
            nodes = main_info_broker.get(
                'node.find', infra_id=node_desc['infra_id'], name=node_name)
            if not nodes:
                raise KeyError(
                    'No node exists with the given name', node_name)
            elif not allnodes and len(nodes) > 1:
                log.warning(
                    'There are multiple nodes with the same node name (%s). ' +
                    'Multiple nodes are ' +
                    ', '.join(item['node_id'] for item in nodes) +
                    '. Choosing the first one as default (%s).',
                    node_name, nodes[0]['node_id'])
            return nodes[0] if not allnodes else nodes

        def cut(inputstr, start, end):
            return inputstr[start:end]

        def getip(node_name):
            nra = main_info_broker.get('node.resource.address',
                  find_node_id(node_name, allnodes=False))
            return nra[0] if isinstance(nra,list) else nra

        def getprivip(node_name):
            return main_info_broker.get('node.resource.ip_address',
                   find_node_id(node_name, allnodes=False))

        def getipall(node_name):
            l = list()
            for node in find_node_id(node_name, allnodes=True):
              nra = main_info_broker.get('node.resource.address', node)
              l = l[:] + [nra[0]] if isinstance(nra,list) else l[:] + [nra]
            return l

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
        source_data['getipall'] = getipall
        source_data['cut'] = cut

        return source_data

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

        context = yaml.load(self.render_template(node_definition, template_data), Loader=yaml.Loader)

        # Amend resolved node with new information
        data = {
            'node_id'     : node_id,
            'name'        : node_desc['name'],
            'infra_id'    : node_desc['infra_id'],
            'attributes'  : self.resolve_attributes(node_desc,
                                                    node_definition,
                                                    template_data,
                                                    context),
            'synch_attrs' : self.extract_synch_attrs(node_desc),
        }
        node_definition.update(data)

@factory.register(ContextSchemaChecker, PROTOCOL_ID)
class DockerContextSchemaChecker(ContextSchemaChecker):
    def __init__(self):
        self.req_keys = ["type", "env", "command"]
        self.opt_keys = ["context_variables"]
    def perform_check(self, data):
        missing_keys = ContextSchemaChecker.get_missing_keys(self, data, self.req_keys)
        if missing_keys:
            msg = "Missing key(s): " + ', '.join(str(key) for key in missing_keys)
            raise SchemaError(msg)
        valid_keys = self.req_keys + self.opt_keys
        invalid_keys = ContextSchemaChecker.get_invalid_keys(self, data, valid_keys)
        if invalid_keys:
            msg = "Unknown key(s): " + ', '.join(str(key) for key in invalid_keys)
            raise SchemaError(msg)
        return True
