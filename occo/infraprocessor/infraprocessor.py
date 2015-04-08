#
# Copyright (C) 2014 MTA SZTAKI
#

""" Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

.. autoclass:: Command
    :members:

.. ifconfig:: api_doc is False

    .. autoclass:: RemotePushStrategy
        :members:
"""

__all__ = ['InfraProcessor',
           'RemoteInfraProcessor', 'RemoteInfraProcessorSkeleton',
           'BasicInfraProcessor',
           'Strategy', 'SequentialStrategy', 'ParallelProcessesStrategy',
           'CreateEnvironment', 'CreateNode', 'DropNode', 'DropEnvironment',
           'Mgmt_SkipUntil']

import logging
import occo.util as util
import occo.util.communication as comm
import occo.util.factory as factory
import occo.infobroker as ib
from node_resolution import resolve_node
import time
import threading
import uuid
import yaml

log = logging.getLogger('occo.infraprocessor')

###################################################
# Strategies to process parallelizable instructions
###

class Strategy(factory.MultiBackend):
    """
    Abstract strategy for processing a batch of *independent* commands.

    :var cancel_event: When supported by an implementation of the strategy,
        this event can be used to abort processing the batch.
    :type cancel_event: :class:`threading.Event`
    """
    def __init__(self):
        self.cancel_event = threading.Event()

    @property
    def cancelled(self):
        """ Returns :data:`True` iff performing the batch should be aborted."""
        return self.cancel_event.is_set()
    def cancel_pending(self):
        """
        Registers that performing the batch should be aborted. It only works
        iff the implementation of the strategy supports it (e.g. a thread pool
        or a sequential iteration can be aborted).
        """
        self.cancel_event.set()

    def perform(self, infraprocessor, instruction_list):
        """
        Perform the instruction list.

        This method must be overridden in the implementations of the strategy.

        :param infraprocessor: The infraprocessor that calls this method.
            Commands are perfomed *on* an infrastructure processor, therefore
            they need a reference to it.
        :param instruction_list: An iterable containing the commands to be
            performed.
        """
        raise NotImplementedError()

@factory.register(Strategy, 'sequential')
class SequentialStrategy(Strategy):
    """Implements :class:`Strategy`, performing the commands sequentially."""
    def perform(self, infraprocessor, instruction_list):
        results = list()
        for i in instruction_list:
            if self.cancelled:
                break
            result = i.perform(infraprocessor)
            results.append(result)
        return results

class PerformThread(threading.Thread):
    """
    Thread object used by :class:`ParallelProcessesStrategy` to perform a
    single command.

    .. todo:: Why did I call it Parallel*Processes* when it used threads?!
    .. todo:: Should implement a parallel strategy that uses actual processes.
        These would be abortable.
    """
    def __init__(self, infraprocessor, instruction):
        super(PerformThread, self).__init__()
        self.infraprocessor = infraprocessor
        self.instruction = instruction
    def run(self):
        try:
            self.result = self.instruction.perform(self.infraprocessor)
        except BaseException:
            log.exception("Unhandled exception in thread:")

@factory.register(Strategy, 'parallel')
class ParallelProcessesStrategy(Strategy):
    """
    Implements :class:`Strategy`, performing the commands in a parallel manner.
    """
    def perform(self, infraprocessor, instruction_list):
        threads = [PerformThread(infraprocessor, i) for i in instruction_list]
        # Start all threads
        for t in threads:
            log.debug('Starting thread for %r', t.instruction)
            t.start()
            log.debug('STARTED Thread for %r', t.instruction)
        results = list()
        # Wait for results
        for t in threads:
            log.debug('Joining thread for %r', t.instruction)
            t.join()
            log.debug('FINISHED Thread for %r', t.instruction)
            results.append(t.result)

@factory.register(Strategy, 'remote')
class RemotePushStrategy(Strategy):
    """
    Implements :class:`Strategy` by simply pushing the instruction list to
    another infrastructure processor. I.e.: to the "real" infrastructure
    processor.

    :param destination_queue: The communication channel to where the instruction
        shall be pushed.
    :type destination_queue:
        :class:`occo.util.communication.comm.AsynchronProducer`

    .. todo:: *IMPORTANT.* Currently, this strategy pushes instructions
        individually---it splits the parallelizable instruction list into
        pieces that will always be executed sequentially. The batch *must* be
        kept together. I recall I've already solved this, but I don't know what
        happened with it :S --Adam
    """
    def __init__(self, destination_queue):
        super(RemotePushStrategy, self).__init__()
        self.queue = destination_queue
    def perform(self, infraprocessor, instruction_list):
        #TODO push as list; keep instructions together
        results = list()
        for i in instruction_list:
            if self.cancelled:
                break
            result = self.queue.push_message(i)
            results.append(result)
        return results

##########################
# Infrastructure Processor
###


class Command(object):
    """
    Abstract definition of a command.

    If they override it, sub-classes must the call ``Command``'s constructor
    to ensure ``timestamp`` is set. This is important because the timestamp is
    used to clear the infrastructure processor queue.
    See: :meth:`InfraProcessor.cancel_pending`.

    :var timestamp: The time of creating the command.
    """
    def __init__(self):
        self.timestamp = time.time()
    def perform(self, infraprocessor):
        """Perform the algorithm represented by this command."""
        raise NotImplementedError()

class InfraProcessor(factory.MultiBackend):
    """
    Abstract definition of the Infrastructure Processor.

    Instead of providing *methods* as primitives, this class implements the
    `Command design pattern`_: primitives are provided as self-contained
    (algorithm + input data) :class:`Command` objects. This way performing
    functions can be 1) batched and 2) delivered through communication
    channels. These primitives can be created with the ``cri_*`` methods.

    :param process_strategy: The strategy used to perform an internally
        independent set of commands.
    :type process_strategy: :class:`Strategy`

    .. _`Command design pattern`: http://en.wikipedia.org/wiki/Command_pattern

    .. automethod:: __enter__
    """
    def __init__(self, process_strategy):
        self.strategy = process_strategy
        self.cancelled_until = 0

    def __enter__(self):
        """
        This can be overridden in a sub-class if necessary.

        .. todo:: This is quite ugly. An Infra Processor doesn't need context
            management per se. This is here because of the remote skeleton, and
            the queues it uses. These queues need this. However, this shouldn't
            affect the infrastructure processor; instead, the queues should be
            initialized in the start_consuming or similar method, as late as
            possible, and invisible to the client. It would be very nice to
            factor this out.
        """
        return self
    def __exit__(self, type, value, tb):
        pass

    def _not_cancelled(self, instruction):
        """
        Decides whether a command has to be considered cancelled; i.e. it
        has arrived after the deadline registered by :meth:`cancel_pending`.

        Negating the function's meaning and its name has spared a lambda
        declaration in :meth:`push_instructions` improving readability.
        """
        return instruction.timestamp > self.cancelled_until

    def push_instructions(self, instructions):
        """
        Performs the given list of independent instructions according to the
        strategy.

        :param instructions: The list of instructions. For convenienve, a
            single instruction can be specified by itself, without enclosing it
            in an iterable.
        :type instructions: An iterable or a single :class:`Command`.
        """
        # If a single Command object has been specified, convert it to an
        # iterable. This way, the client code can remain more simple if a
        # single command has to be specified; while the strategy can perform
        # `instructions` uniformly as an iterable.
        instruction_list = \
            instructions if hasattr(instructions, '__iter__') \
            else (instructions,)
        # TODO Reseting the event should probably be done by the strategy.
        self.strategy.cancel_event.clear()
        # Don't bother with commands known to be already cancelled.
        filtered_list = list(filter(self._not_cancelled, instruction_list))
        log.debug('Filtered list: %r', filtered_list)
        return self.strategy.perform(self, filtered_list)

    def cri_create_env(self, environment_id):
        """ Create a primitive that will create an infrastructure instance. """
        raise NotImplementedError()
    def cri_create_node(self, node):
        """ Create a primitive that will create an node instance. """
        raise NotImplementedError()
    def cri_drop_node(self, node_id):
        """ Create a primitive that will delete a node instane. """
        raise NotImplementedError()
    def cri_drop_environment(self, environment_id):
        """ Create a primitive that will delete an infrastructure instance. """
        raise NotImplementedError()

    def cancel_pending(self, deadline):
        """
        Registers that commands up to a specific time should be considered
        cancelled.

        :param deadline: The strategy will try to cancel/abort/ignore
            performing the commands that has been created before this time.
        :type deadline: :class:`int`, unix timestamp
        """
        self.cancelled_until = deadline
        self.strategy.cancel_pending()

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

        # Wait for the node to start
        while True:
            # TODO add timeout
            status = ib.get('node.state', instance_data)
            # TODO handle other statuses too (error, failure, etc.)
            # TODO should handle the statuses using an abstract factory or some
            #      other smart solution based on the status string, because
            #      elif-s will get quickly out of hand
            if status == 'running:ready':
                break
            log.debug("Node status: '%s'; waiting...", status)
            time.sleep(infraprocessor.poll_delay)
        log.debug("Node '%s' started; proceeding", node_id)

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
                 process_strategy=SequentialStrategy(),
                 poll_delay=10,
                 **config):
        super(BasicInfraProcessor, self) \
            .__init__(process_strategy=process_strategy)
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

##################
# Remote interface
##

class Mgmt_SkipUntil(Command):
    """
    Implementation of cancelling commands until a given time.

    :param int deadline: Unix timestamp that will be compared to
        :class:`Command`-s ``timestamp``.

    Performing this will essentially clean the queue and the Infrastructure
    Processor of pending instructions.

    The Infrastructure Processor will *try* to abort and undo instructions
    already being executed. Otherwise, it will wait until they are finalized.

    The Infrastructure Processor will then disregard new instructions up to the
    given deadline (based on their ``timestamp``).

    This instruction is meant to be delivered through the *synchronous*
    management queue of the :class:`RemoteInfraProcessor`. When this
    instruction has been executed, the Infrastructure Processor will be idle,
    and the queue will be virtually empty---the infrastructure will be in a
    non-transient state. Thus, instructions generated by the :ref:`Enactor
    <enactor>` at this point will be executed without interference.

    """
    def __init__(self, deadline):
        Command.__init__(self)
        self.deadline = deadline
    def perform(self, infraprocessor):
        infraprocessor.cancel_upcoming(self.deadline)

@factory.register(InfraProcessor, 'remote')
class RemoteInfraProcessor(BasicInfraProcessor):
    """
    A remote implementation of :class:`InfraProcessor`.

    The exact same command objects are created by this class as that by the
    :class:`BasicInfraProcessor`. The difference is that this class uses the
    :class:`RemotePushStrategy` to perform instructions.

    This class communicates with a :class:`RemoteInfraProcessorSkeleton`
    through a :class:`~occo.util.communication.comm.AsynchronProducer`.

    :param destination_queue_cfg: The configuration for the backend
        communication channel.

    .. todo:: Rethink queue context management.
        See :meth:`InfraProcessor.__enter__`.
    """
    def __init__(self, destination_queue_cfg):
        # Calling only the abstract IP's __init__
        # (and skipping BasicInfraProcessor.__init__) is intentional:
        #
        # Command classes must be inherited (hence the BasicInfraProcessor
        # parent), but this class does not need the IP's backends (infobroker,
        # cloudhandler, etc.)
        InfraProcessor.__init__(
            self, process_strategy=RemotePushStrategy(
                    comm.AsynchronProducer(**destination_queue_cfg)))

    def __enter__(self):
        self.strategy.queue.__enter__()
        return self
    def __exit__(self, type, value, tb):
        self.strategy.queue.__exit__(type, value, tb)

    def cancel_pending(self, deadline):
        """
        Implementation of :meth:`InfraProcessor.cancel_pending` with
        :class:`Mgmt_SkipUntil`.
        """
        self.push_instructions([Mgmt_SkipUntil(deadline)])

class RemoteInfraProcessorSkeleton(object):
    """
    Skeleton_ part of the Infrastructure Processor RMI solution.

    A ~ object reads serialized :class:`Command`\ s from the backend queue,
    deserializes them, and dispatches them to the backend ("real")
    infrastructure processor. These commands are sent by a client application
    through a :class:`RemoteInfraProcessor` object.

    :param backend_ip: The "real" *I*\ nfrastructure *P*\ rocessor (hence
        ``_ip``).  Actually, this may be another :class:`RemoteInfraProcessor`
        if necessary for some twisted reason...
    :type backend_ip: :class:`InfraProcessor`

    :param ip_queue_cfg: Configuration intended for the backend
        :class:`~occo.util.communication.comm.EventDrivenConsumer` for the
        *regular* instruction queue. This queue is expected to be an
        asynchronous message queue.

    :param control_queue_cfg: Configuration intended for the backend
        :class:`~occo.util.communication.comm.EventDrivenConsumer` for the
        *management* instruction queue. This queue is expected to be an RMI
        queue.

    :param cancel_event: Event object used to abort processing. If :data:`None`,
        :meth:`start_consuming` will yield immediately after polling both
        queues once. In this case, it must be called by the client repeatedly.
    :type cancel_event: :class:`threading.Event`

    .. _skeleton:
        http://stackoverflow.com/questions/8586206/what-is-stub-on-the-server-and-what-does-skeleton-mean
    """
    def __init__(self, backend_ip, ip_queue_cfg, control_queue_cfg,
                 cancel_event=None):
        self.backend_ip = backend_ip
        self.cancel_event = cancel_event

        # Ensure that these consumers are non-looping.
        # This is because polling has to alternate on them. If one of them is
        # looping, the other will never be polled.
        ip_queue_cfg['cancel_event'] = None
        control_queue_cfg['cancel_event'] = None

        self.ip_consumer = comm.EventDrivenConsumer(
            self.process_ip_msg, **ip_queue_cfg)
        self.control_consumer = comm.EventDrivenConsumer(
            self.process_control_msg, **control_queue_cfg)

    def __enter__(self):
        # Start the contexts of the consumers transactionally
        self.ip_consumer.__enter__()
        try:
            self.control_consumer.__enter__()
        except:
            # This is a "ROLLBACK"
            self.control_consumer.__exit__(None, None, None)
            raise
        return self
    def __exit__(self, type, value, tb):
        try:
            self.ip_consumer.__exit__(type, value, tb)
        finally:
            self.control_consumer.__exit__(type, value, tb)

    @property
    def cancelled(self):
        """ Returns true iff :meth:`start_consuming` should yield. """
        return self.cancel_event is None or self.cancel_event.is_set()

    def start_consuming(self):
        """
        Starts processing the input queues.

        This method will loop if ``self.cancel_event`` is specified, until a
        signal arrives through this object.

        If ``self.cancel_event`` is :data:`None`, this method will yield after
        polling both queues.

        The control queue is polled first so urgent control messages---like,
        e.g. :class:`Mgmt_SkipUntil`---arrive before starting work needlessly.
        """
        while not self.cancelled:
            log.debug("Processing control messages")
            self.control_consumer.start_consuming()
            log.debug("Processing normal messages")
            self.ip_consumer.start_consuming()
            time.sleep(0) # Yield CPU

    def process_ip_msg(self, instruction_list, *args, **kwargs):
        """ Callback function for the regular queue. """
        # Return value not needed -- this is NOT an rpc queue
        log.debug("Received normal message")
        self.backend_ip.push_instructions(instruction_list)

    def process_control_msg(self, instruction, *args, **kwargs):
        """ Callback function for the control queue. """
        # This is an RPC queue.
        # Control messages are immediately performed, disregarding
        # their timestamp and skip_until.
        log.debug("Received control message")
        try:
            retval = instruction.perform(self.backend_ip)
            return comm.Response(200, retval)
        except Exception as ex:
            return comm.ExceptionResponse(500, ex)
