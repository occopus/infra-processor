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

""" Chef resolver for node definitions.

.. _`connect`: https://gitlab.lpds.sztaki.hu/cloud-orchestrator/connect-cookbook

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

from __future__ import absolute_import

__all__ = ['CloudinitResolver']

import logging
import occo.util as util
import occo.exceptions as exceptions
import occo.util.factory as factory
import sys
from ruamel import yaml
import jinja2
from occo.infraprocessor.node_resolution import Resolver, ContextSchemaChecker
from occo.exceptions import SchemaError
import occo.infobroker as ib

PROTOCOL_ID = 'cloudinit'

log = logging.getLogger('occo.infraprocessor.node_resolution.cloudinit')
datalog = logging.getLogger('occo.data.infraprocessor.node_resolution.cloudinit')

@factory.register(Resolver, PROTOCOL_ID)
class CloudinitResolver(Resolver):
    """
    Implementation of :class:`Resolver` for implementations for `cloud-init`_ .

    This resolver updates the node definition with information required by
    any kind of :ref:`Cloud Handler <resourcehandler>` that uses cloud-init; 
    for example the :class:`~occo.resourcehandler.backend.ec2.EC2ResourceHandler`.

    .. todo:: This aspect of OCCO (resolving) must be thought over.

    .. _`cloud-init`: https://cloudinit.readthedocs.org/en/latest/
    """

    def extract_template(self, node_definition):

        def context_list():
            # `context_template` is also removed from the definition, as
            # it will be replaced with the rendered `context`
            context_section = node_definition.get('contextualisation', None)
            yield ('node_definition',
                   context_section.pop('context_template', None))

            yield 'default', ''

        src, template = util.find_effective_setting(context_list())

        if isinstance(template, dict):
              template="#cloud-config\n"+yaml.dump(template,default_flow_style=False)

        datalog.debug('Context template from %s:\n%s', src, template)

        return jinja2.Template(template)

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

        def getip(node_name):
            return main_info_broker.get('node.resource.address',
                   find_node_id(node_name, allnodes=False))

        def getipall(node_name):
            return [ main_info_broker.get('node.resource.address', node)
                     for node in find_node_id(node_name, allnodes=True) ]
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

        #state = main_info_broker.get('node.find', infra_id=node_desc['infra_id'])
        #for node in state:
        #    source_data[node['node_description']['name']] = \
        #    dict(ip=main_info_broker.get('node.resource.address', node))

        return source_data

    def check_if_cloud_config(self, node_definition):
        """
        Process context iff it's a cloud-config.
        """
        # TODO: Maybe this condition can be imporoved? Need to verify.
        if not node_definition['context'].startswith('#cloud-config'):
            return

        # Verify that the context *is* parsable by YAML. Otherwise, cloud-init
        # will fail silently.
        try:
            yaml.load(node_definition['context'],Loader=yaml.Loader)
        except yaml.YAMLError as e:
            if hasattr(e, 'problem_mark'):
                msg=('Schema error in context of '
                     'node definition at line {0}.').format(e.problem_mark.line)
            else:
                msg='Schema error in context of node definition.'

            raise \
                exceptions.NodeContextSchemaError(
                    node_definition=node_definition, reason=e, msg=msg), \
                None, sys.exc_info()[2]

    def check_template(self, node_definition):
        """
        Checks the context part of node definition.

        :raises occo.exceptions.orchestration.NodeContextSchemaError: if the
            contextualization is invalid.
        """
        self.check_if_cloud_config(node_definition)
        # Checks for other types of contextualization can be listed here

    def render_template(self, node_definition, template_data):
        """Renders the template pertaining to the node definition"""
        template = self.extract_template(node_definition)
        return template.render(**template_data)

    def resolve_config_management_section(self, node_definition, template_data):
        #datalog.info("ConfigManagerSection before resolution: \"%r\"\n",node_definition.get('config_management'))
        template = jinja2.Template(yaml.dump(node_definition.get('config_management')))
        ret = yaml.load(template.render(**template_data),Loader=yaml.Loader)
        #datalog.info("ConfigManagerSection after resolution: \"%r\"\n",ret)
        return ret

    def _resolve_node(self, node_definition):
        """
        Implementation of :meth:`Resolver.resolve_node`.
        """

        # Shorthands
        node_desc = self.node_description
        #ib = self.info_broker
        node_id = self.node_id
        template_data = self.assemble_template_data(node_desc, node_definition)

        # Resolve Config Manager section in node_definition if exists:
        if "config_management" in node_definition:
          resolved_cms = self.resolve_config_management_section(node_definition, template_data)
          # Update node definition with resolved config management section
          node_definition['config_management'].update(resolved_cms)
          # Query attributes from CM to be used during resolution
          cm_attributes = ib.main_configmanager.resolve_attributes(node_definition)
          template_data.update(cm_attributes)

        # Amend resolved node with new information
        data = {
            'node_id'     : node_id,
            'name'        : node_desc['name'],
            'infra_id'    : node_desc['infra_id'],
            'context'     : self.render_template(node_definition,
                                                 template_data),
            'attributes'  : self.resolve_attributes(node_desc,
                                                    node_definition,
                                                    template_data),
            'synch_attrs' : self.extract_synch_attrs(node_desc),
        }
        node_definition.update(data)

        # Check context
        self.check_template(node_definition)

@factory.register(ContextSchemaChecker, PROTOCOL_ID)
class CloudinitSchemaChecker(ContextSchemaChecker):
    def __init__(self):
        self.req_keys = ["type","context_template"]
        self.opt_keys = ["attributes"]
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

