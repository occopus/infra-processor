#
# Copyright (C) 2014 MTA SZTAKI
#

""" Basic Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>
"""

__all__ = ['BasicInfraProcessor',
           'CreateEnvironment', 'CreateNode', 'DropNode', 'DropEnvironment']

import logging
import occo.util.factory as factory
import occo.infobroker as ib
from node_resolution import resolve_node
import uuid
import yaml
from occo.infraprocessor import InfraProcessor, Command
from occo.infraprocessor.strategy import Strategy

log = logging.getLogger('occo.infraprocessor.basic')

###############
## IP Commands

class CreateEnvironment(Command):
    """
    Implementation of infrastructure creation using a
    :ref:`service composer <servicecomposer>`.

    :param str environment_id: The identifier of the infrastructure instance.

    The ``environment_id`` is a unique identifier pre-generated by the
    :ref:`Compiler <compiler>`. The infrastructure will be instantiated with
    this identifier.
    """
    def __init__(self, environment_id):
        Command.__init__(self)
        self.environment_id = environment_id
    def perform(self, infraprocessor):
        return infraprocessor.servicecomposer.create_environment(
            self.environment_id)

class CreateNode(Command):
    """
    Implementation of node creation using a
    :ref:`service composer <servicecomposer>` and a
    :ref:`cloud handler <cloudhandler>`.

    :param node: The description of the node to be created.
    :type node: :ref:`nodedescription`

    """
    def __init__(self, node):
        Command.__init__(self)
        self.node = node

    def perform(self, infraprocessor):
        """
        Start the node.

        This implementation is **incomplete**. We need to:

        .. todo:: Handle errors when creating the node (if necessary; it is
            possible that they are best handled in the InfraProcessor itself).

        .. warning:: Does the parallelized strategy propagate errors
            properly? Must verify!

        .. todo::
            Handle all known possible statuses

        .. todo:: We synchronize on the node becoming completely ready
            (started, configured). We need a **timeout** on this.

        """

        # Quick-access references
        ib = infraprocessor.ib
        node = self.node

        log.debug('Performing CreateNode on node {\n%s}',
                  yaml.dump(node, default_flow_style=False))

        # Internal identifier of the node instance
        node_id = str(uuid.uuid4())
        # Resolve all the information required to instantiate the node using
        # the abstract description and the UDS/infobroker
        resolved_node = resolve_node(ib, node_id, node)
        log.debug("Resolved node description:\n%s",
                  yaml.dump(resolved_node, default_flow_style=False))

        # Create the node based on the resolved information
        infraprocessor.servicecomposer.register_node(resolved_node)
        instance_id = infraprocessor.cloudhandler.create_node(resolved_node)

        # Information specifying a running node instance
        # See: :ref:`instancedata`.
        instance_data = dict(node_id=node_id,
                             backend_id=resolved_node['backend_id'],
                             user_id=node['user_id'],
                             instance_id=instance_id)

        # Although all information can be extraced from the system dynamically,
        # we keep an internal record on the state of the infrastructure for
        # time efficiency.
        infraprocessor.uds.register_started_node(
            node['environment_id'], node['name'], instance_data)

        log.info(
            "Node %s/%s/%s received IP address: %r",
            node['environment_id'], node['name'], node_id,
            ib.get('node.cloud_attribute', 'ipaddress', instance_data))

        from occo.infraprocessor.synchronization import wait_for_node

        try:
            # TODO Add timeout
            wait_for_node(node, resolved_node, instance_data, infraprocessor,
                          infraprocessor.poll_delay)
        #TODO Handle other errors
        except Exception:
            log.exception('Unhandled exception when waiting for node:')
            # TODO: Undo damage
            raise
        else:
            log.info("Node %s/%s/%s has started",
                     node['environment_id'], node['name'], node_id)

        return instance_data

class DropNode(Command):
    """
    Implementation of node deletion using a
    :ref:`service composer <servicecomposer>` and a
    :ref:`cloud handler <cloudhandler>`.

    :param instance_data: The description of the node instance to be deleted.
    :type instance_data: :ref:`instancedata`

    """
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    def perform(self, infraprocessor):
        infraprocessor.cloudhandler.drop_node(self.instance_data)
        infraprocessor.servicecomposer.drop_node(self.instance_data)

class DropEnvironment(Command):
    """
    Implementation of infrastructure deletion using a
    :ref:`service composer <servicecomposer>`.

    :param str environment_id: The identifier of the infrastructure instance.
    """
    def __init__(self, environment_id):
        Command.__init__(self)
        self.environment_id = environment_id
    def perform(self, infraprocessor):
        infraprocessor.servicecomposer.drop_environment(self.environment_id)

####################
## IP implementation

@factory.register(InfraProcessor, 'basic')
class BasicInfraProcessor(InfraProcessor):
    """
    Implementation of :class:`InfraProcessor` using the primitives defined in
    this module.

    :param user_data_store: Database manipulation.
    :type user_data_store: :class:`~occo.infobroker.UDS`

    :param cloudhandler: Cloud access.
    :type cloudhandler: :class:`~occo.cloudhandler.cloudhandler.CloudHandler`

    :param servicecomposer: Service composer access.
    :type servicecomposer:
        :class:`~occo.servicecomposer.servicecomposer.ServiceComposer`

    :param process_strategy: Plug-in strategy for performing an independent
        batch of instructions.
    :type process_strategy: :class:`Strategy`

    :param int poll_delay: Node creation is synchronized on the node becoming
        completely operational. This condition has to be polled in
        :meth:`CreateNode.perform`. ``poll_delay`` is the number of seconds to
        wait between polls.
    """
    def __init__(self, user_data_store,
                 cloudhandler, servicecomposer,
                 process_strategy='sequential',
                 poll_delay=10,
                 **config):
        super(BasicInfraProcessor, self).__init__(
                process_strategy=Strategy.instantiate(process_strategy))
        self.__dict__.update(config)
        self.ib = ib.main_info_broker
        self.uds = user_data_store
        self.cloudhandler = cloudhandler
        self.servicecomposer = servicecomposer
        self.poll_delay = poll_delay

    def cri_create_env(self, environment_id):
        return CreateEnvironment(environment_id)
    def cri_create_node(self, node):
        return CreateNode(node)
    def cri_drop_node(self, node_id):
        return DropNode(node_id)
    def cri_drop_environment(self, environment_id):
        return DropEnvironment(environment_id)
