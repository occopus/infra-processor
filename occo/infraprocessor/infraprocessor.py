#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['RemoteInfraProcessor', 'RemoteInfraProcessorSkeleton', 'InfraProcessor',
           'Strategy', 'SequentialStrategy', 'ParallelProcessesStrategy',
           'CreateEnvironment', 'CreateNode', 'DropNode', 'DropEnvironment',
           'Mgmt_SkipUntil']

import logging
import occo.util as util
import occo.util.communication as comm
import time
import threading
import uuid
import yaml

log = logging.getLogger('occo.infraprocessor')

###################################################
# Strategies to process parallelizable instructions
###

class Strategy(object):
    def __init__(self):
        self.cancel_event = threading.Event()

    @property
    def cancelled(self):
        return self.cancel_event.is_set()
    def cancel_pending(self):
        # Where applicable
        self.cancel_event.set()

    def perform(self, infraprocessor, instruction_list):
        raise NotImplementedError()

class SequentialStrategy(Strategy):
    def perform(self, infraprocessor, instruction_list):
        for i in instruction_list:
            if self.cancelled:
                break
            i.perform(infraprocessor)

class PerformThread(threading.Thread):
    def __init__(self, infraprocessor, instruction):
        super(PerformThread, self).__init__()
        self.infraprocessor = infraprocessor
        self.instruction = instruction
    def run(self):
        try:
            self.instruction.perform(self.infraprocessor)
        except BaseException:
            log.exception("Unhandled exception in thread:")
class ParallelProcessesStrategy(Strategy):
    def perform(self, infraprocessor, instruction_list):
        threads = [PerformThread(infraprocessor, i) for i in instruction_list]
        for t in threads:
            log.debug('Starting thread for %r', t.instruction)
            t.start()
            log.debug('STARTED Thread for %r', t.instruction)
        for t in threads:
            log.debug('Joining thread for %r', t.instruction)
            t.join()
            log.debug('FINISHED Thread for %r', t.instruction)

class RemotePushStrategy(Strategy):
    def __init__(self, destination_queue):
        super(RemotePushStrategy, self).__init__()
        self.queue = destination_queue
    def perform(self, infraprocessor, instruction_list):
        #TODO push as list; keep instructions together
        for i in instruction_list:
            if self.cancelled:
                break
            self.queue.push_message(i)

##########################
# Infrastructure Processor
###


class Command(object):
    def __init__(self):
        self.timestamp = time.time()
    def perform(self, infraprocessor):
        raise NotImplementedError()

class AbstractInfraProcessor(object):
    def __init__(self, process_strategy):
        self.strategy = process_strategy
        self.cancelled_until = 0

    def __enter__(self):
        return self
    def __exit__(self, type, value, tb):
        pass

    def _not_cancelled(self, instruction):
        return instruction.timestamp > self.cancelled_until

    def push_instructions(self, instructions):
        # Make `instructions' iterable if necessary
        instruction_list = \
            instructions if hasattr(instructions, '__iter__') \
            else (instructions,)
        self.strategy.cancel_event.clear()
        filtered_list = list(filter(self._not_cancelled, instruction_list))
        log.debug('Filtered list: %r', filtered_list)
        self.strategy.perform(self, filtered_list)

    def cri_create_env(self, environment_id):
        raise NotImplementedError()
    def cri_create_node(self, node):
        raise NotImplementedError()
    def cri_drop_node(self, node_id):
        raise NotImplementedError()
    def cri_drop_environment(self, environment_id):
        raise NotImplementedError()

    def cancel_pending(self, deadline):
        self.cancelled_until = deadline
        self.strategy.cancel_pending()

###############
## IP Commands

class CreateEnvironment(Command):
    def __init__(self, environment_id):
        Command.__init__(self)
        self.environment_id = environment_id
    def perform(self, infraprocessor):
        infraprocessor.servicecomposer.create_environment(self.environment_id)

class CreateNode(Command):
    def __init__(self, node):
        Command.__init__(self)
        self.node = node
    def resolve_node(self, ib, node_id, node_description):
        # Use `node' for short
        node = node_description

        # Resolve node definition
        resolved_node = ib.get('node.definition',
                               node['type'], node.get('backend_id'))
        # TODO: Alternative, future version:
        # resolved_node = brokering_service.select_implementation(
        #                                node['type'], node.get('backend_id'))
        # The brokering servie will call ib.get() as necessary.

        # Amend resolved node with basic information
        resolved_node['id'] = node_id
        resolved_node['name'] = node['name']
        resolved_node['environment_id'] = node['environment_id']
        # Resolve backend-specific authentication information
        resolved_node['auth_data'] = ib.get('backends.auth_data',
                                            resolved_node['backend_id'],
                                            node['user_id'])

        return resolved_node
    def perform(self, infraprocessor):
        ib = infraprocessor.ib
        node = self.node
        log.debug('Performing CreateNode on node {\n%s}',
                  yaml.dump(node, default_flow_style=False))

        node_id = str(uuid.uuid4())
        resolved_node = self.resolve_node(ib, node_id, node)
        log.debug("Resolved node description:\n%s",
                  yaml.dump(resolved_node, default_flow_style=False))

        infraprocessor.servicecomposer.register_node(resolved_node)
        instance_id = infraprocessor.cloudhandler.create_node(resolved_node)

        instance_data = dict(node_id=node_id,
                             backend_id=resolved_node['backend_id'],
                             user_id=node['user_id'],
                             instance_id=instance_id)

        infraprocessor.uds.register_started_node(
            node['environment_id'], node['name'], instance_data)

        # TODO synchronize on node state here

class DropNode(Command):
    def __init__(self, instance_data):
        Command.__init__(self)
        self.instance_data = instance_data
    def perform(self, infraprocessor):
        infraprocessor.cloudhandler.drop_node(self.instance_data)
        infraprocessor.servicecomposer.drop_node(self.instance_data)

class DropEnvironment(Command):
    def __init__(self, environment_id):
        Command.__init__(self)
        self.environment_id = environment_id
    def perform(self, infraprocessor):
        infraprocessor.servicecomposer.drop_environment(self.environment_id)

####################
## IP implementation

class InfraProcessor(AbstractInfraProcessor):
    def __init__(self, infobroker, user_data_store,
                 cloudhandler, servicecomposer,
                 process_strategy=SequentialStrategy(),
                 **config):
        super(InfraProcessor, self).__init__(process_strategy=process_strategy)
        self.__dict__.update(config)
        self.ib = infobroker
        self.uds = user_data_store
        self.cloudhandler = cloudhandler
        self.servicecomposer = servicecomposer

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
    def __init__(self, deadline):
        Command.__init__(self)
        self.deadline = deadline
    def perform(self, infraprocessor):
        infraprocessor.cancel_upcoming(self.deadline)

class RemoteInfraProcessor(InfraProcessor):
    def __init__(self, destination_queue_cfg):
        # Calling only the AbstractIP's __init__
        # (and skipping InfraProcessor.__init__) is intentional:
        #
        # Command classes must be inherited (hence the InfraProcessor parent),
        # but this class does not need the IP's backends (infobroker,
        # cloudhandler, etc.)
        AbstractInfraProcessor.__init__(
            self, process_strategy=RemotePushStrategy(
                    comm.AsynchronProducer(**destination_queue_cfg)))

    def __enter__(self):
        self.strategy.queue.__enter__()
        return self
    def __exit__(self, type, value, tb):
        self.strategy.queue.__exit__(type, value, tb)

    def cancel_pending(self, deadline):
        self.push_instructions([Mgmt_SkipUntil(deadline)])

class RemoteInfraProcessorSkeleton(object):
    def __init__(self, backend_ip, ip_queue_cfg, control_queue_cfg, cancel_event=None):
        self.backend_ip = backend_ip
        self.cancel_event = cancel_event

        # Ensure that these consumers are non-looping
        ip_queue_cfg['cancel_event'] = None
        control_queue_cfg['cancel_event'] = None

        self.ip_consumer = comm.EventDrivenConsumer(
            self.process_ip_msg, **ip_queue_cfg)
        self.control_consumer = comm.EventDrivenConsumer(
            self.process_control_msg, **control_queue_cfg)

    def __enter__(self):
        self.ip_consumer.__enter__()
        try:
            self.control_consumer.__enter__()
        except:
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
        return self.cancel_event is None or self.cancel_event.is_set()

    def start_consuming(self):
        while not self.cancelled:
            log.debug("Processing control messages")
            self.control_consumer.start_consuming()
            log.debug("Processing normal messages")
            self.ip_consumer.start_consuming()
            time.sleep(0) # Yield CPU

    def process_ip_msg(self, instruction_list, *args, **kwargs):
        # Return value not needed -- this is NOT an rpc queue
        log.debug("Received normal message")
        self.backend_ip.push_instructions(instruction_list)

    def process_control_msg(self, instruction, *args, **kwargs):
        # This is an RPC queue.
        # Control messages are immediately performed, disregarding
        # their timestamp and skip_until.
        log.debug("Received control message")
        try:
            retval = instruction.perform(self.backend_ip)
            return comm.Response(200, retval)
        except Exception as ex:
            return comm.ExceptionResponse(500, ex)
