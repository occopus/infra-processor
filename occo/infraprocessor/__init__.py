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

""" Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

.. autoclass:: Command
    :members:

"""

import logging
import occo.util.factory as factory
import occo.infobroker as ib
from occo.infraprocessor.strategy import Strategy

log = logging.getLogger('occo.infraprocessor')

class Command(object):
    """
    Abstract definition of an InfraProcessor command using the Command design
    pattern.

    Arguments should be passed through the constructor object, which should then store the
    """
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
    """
    def __init__(self, process_strategy):
        self.strategy = Strategy.from_config(process_strategy)
        log.debug('Initialized InfraProcessor with strategy %s', self.strategy)

    def push_instructions(self, infra_id, instructions):
        """
        Performs the given list of independent instructions according to the
        strategy.

        :param instructions: The list of instructions. For convenience, a
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
        log.debug('Pushing instruction list: %r', instruction_list)
        rv = self.strategy.perform(self, instruction_list)
        ib.main_eventlog.infrastructure_updated(infra_id)
        return rv

    def cri_create_infrastructure(self, infra_id):
        """ Create a primitive that will create an infrastructure instance. """
        raise NotImplementedError()
    def cri_create_node(self, node_description):
        """ Create a primitive that will create an node instance. """
        raise NotImplementedError()
    def cri_drop_node(self, instance_data):
        """ Create a primitive that will delete a node instane. """
        raise NotImplementedError()
    def cri_drop_infrastructure(self, infra_id):
        """ Create a primitive that will delete an infrastructure instance. """
        raise NotImplementedError()

    def cancel_pending(self):
        """
        Cancels pending opartions.
        """
        log.info('Cancelling pending instructions')
        self.strategy.cancel_pending()
