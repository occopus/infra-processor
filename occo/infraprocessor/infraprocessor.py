#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['RemoteInfraProcessor', 'InfraProcessor',
           'Strategy', 'SequentialStrategy', 'ParalallelProcessesStrategy']

import logging
import occo.util as util
import occo.util.communication as comm

log = logging.getLogger('occo.util.ip')

# Strategies to process parallelizable instructions

class Strategy(object):
    def perform(self, infraprocessor, instruction_list):
        raise NotImplementedError()

class SequentialStrategy(Strategy):
    def perform(self, infraprocessor, instruction_list):
        for i in instruction_list:
            i.perform(infraprocessor)

class ParallelProcessesStrategy(Strategy):
    def perform(self, infraprocessor, instruction_list):
        raise NotImplementedError() #TODO

class RemotePushStrategy(Strategy):
    def __init__(self, destination_queue):
        self.queue = destination_queue
    def perform(self, infraprocessor, instruction_list):
        for i in instruction_list:
            self.queue.push_message(i)

# Infrastructure Processor

class Command(object):
    def perform(self, infraprocessor):
        raise NotImplementedError()

class AbstractInfraProcessor(object):
    def __init__(self, process_strategy):
        self.strategy = process_strategy

    def push_instructions(self, instruction_list):
        self.strategy.perform(self, instruction_list)

    def cri_create_env(self, environment_id):
        return self.__class__.CreateEnvironment(environment_id)
    def cri_create_node(self, node):
        return self.__class__.CreateNode(node)
    def cri_drop_node(self, node_id):
        return self.__class__.DropNode(node_id)
    def cri_drop_environment(self, environment_id):
        return self.__class__.DropEnvironment(environment_id)

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
            infraprocessor.servicecomposer.create_environment(self.environment_name)

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

class RemoteInfraProcessor(InfraProcessor):
    def __init__(self, destination_queue):
        # Calling only the AbstractIP's __init__ is intentional.
        AbstractInfraProcessor.__init__(
            self, process_strategy=RemotePushStrategy(destination_queue))
