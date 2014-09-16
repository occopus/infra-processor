#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['RemoteInfraProcessor', 'InfraProcessor',
           'Strategy', 'SequentialStrategy', 'ParalallelProcessesStrategy']

import logging
import occo.util as util
import occo.util.communication as comm
import time
import threading

log = logging.getLogger('occo.util.ip')

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

class ParallelProcessesStrategy(Strategy):
    def perform(self, infraprocessor, instruction_list):
        raise NotImplementedError() #TODO

class RemotePushStrategy(Strategy):
    def __init__(self, destination_queue):
        self.queue = destination_queue
    def perform(self, infraprocessor, instruction_list):
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

    def _not_cancelled(self, instruction):
        return instruction.timestamp > self.cancelled_until

    def push_instructions(self, instruction_list):
        self.strategy.cancel_event.clear()
        filtered_list = filter(self.not_cancelled, instruction_list)
        self.strategy.perform(self, filtered_list)

    def cri_create_env(self, environment_id):
        return self.__class__.CreateEnvironment(environment_id)
    def cri_create_node(self, node):
        return self.__class__.CreateNode(node)
    def cri_drop_node(self, node_id):
        return self.__class__.DropNode(node_id)
    def cri_drop_environment(self, environment_id):
        return self.__class__.DropEnvironment(environment_id)

    def cancel_pending(self, deadline):
        self.cancelled_until = deadline
        self.strategy.cancel_pending()

class InfraProcessor(AbstractInfraProcessor):
    def __init__(self, infobroker, cloudhandler, servicecomposer,
                 process_strategy=SequentialStrategy()):
        super(InfraProcessor, self).__init__(process_strategy=process_strategy)
        self.ib = infobroker
        self.cloudhandler = cloudhandler
        self.servicecomposer = servicecomposer

    class CreateEnvironment(Command):
        def __init__(self, environment_name):
            super(CreateEnvironment, self).__init__()
            self.environment_name = environment_name
        def perform(self, infraprocessor):
            infraprocessor.servicecomposer.create_environment(
                self.environment_name)

    class CreateNode(Command):
        def __init__(self, node):
            super(CreateNode, self).__init__()
            self.node = node
        def perform(self, infraprocessor):
            self.node.unique_id = str(uuid.uuid4())
            infraprocessor.servicecomposer.register_node(node)
            infraprocessor.cloudhandler.create_node(node)

    class DropNode(Command):
        def __init__(self, node_id):
            super(DropNode, self).__init__()
            self.node_id = node_id
        def perform(self, infraprocessor):
            infraprocessor.cloudhandler.drop_node(self.node_id)
            infraprocessor.servicecomposer.drop_node(self.node_id)

    class DropEnvironment(Command):
        def __init__(self, environment_id):
            super(DropEnvironment, self).__init__()
            self.environment_id = environment_id
        def perform(self, infraprocessor):
            infraprocessor.servicecomposer.drop_environment(self.environment_id)

##################
# Remote interface
##

class RemoteInfraProcessor(InfraProcessor):
    def __init__(self, destination_queue):
        # Calling only the AbstractIP's __init__
        # (and skipping InfraProcessor.__init__) is intentional:
        #
        # Command classes must be inherited (hence the InfraProcessor parent),
        # but this class does not need the IP's backends (infobroker,
        # cloudhandler, etc.)
        AbstractInfraProcessor.__init__(
            self, process_strategy=RemotePushStrategy(destination_queue))

    def cancel_pending(self, deadline):
        self.push_instructions([Mgmt_SkipUntil(deadline)])

    class Mgmt_SkipUntil(Command):
        def __init__(self, deadline):
            super(Mgmt_SkipUntil, self).__init__()
            self.deadline = deadline
        def perform(self, infraprocessor):
            infraprocessor.cancel_upcoming(self.deadline)


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

    @property
    def cancelled(self):
        return self.cancel_event is None or self.cancel_event.is_set()

    def start_consuming(self):
        while not self.cancelled:
            self.control_consumer.start_consuming()
            self.ip_consumer.start_consuming()

    def process_ip_msg(self, instruction_list, *args, **kwargs):
        # Return value not needed -- this is NOT an rpc queue
        try:
            self.backend_ip.perform(instruction_list)
        except Exception as ex:
            pass

    def process_control_msg(self, instruction, *args, **kwargs):
        # This is an RPC queue.
        # Control messages are immediately performed, disregarding
        # their timestamp and skip_until.
        try:
            retval = instruction.perform(self.backend_ip)
            return comm.Response(200, retval)
        except Exception as ex:
            return comm.ExceptionResponse(500, ex)
