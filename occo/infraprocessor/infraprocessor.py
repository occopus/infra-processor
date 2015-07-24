#
# Copyright (C) 2014 MTA SZTAKI
#

""" Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

.. autoclass:: Command
    :members:

"""

import logging
import occo.util.factory as factory
from occo.infraprocessor.strategy import Strategy
import time

log = logging.getLogger('occo.infraprocessor')

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
        self.strategy = Strategy.from_config(process_strategy)
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
        # Don't bother with commands known to be already cancelled.
        filtered_list = list(filter(self._not_cancelled, instruction_list))
        log.debug('Filtered list: %r', filtered_list)
        return self.strategy.perform(self, filtered_list)

    def cri_create_infrastructure(self, infra_id):
        """ Create a primitive that will create an infrastructure instance. """
        raise NotImplementedError()
    def cri_create_node(self, node_description):
        """ Create a primitive that will create an node instance. """
        raise NotImplementedError()
    def cri_drop_node(self, node_id):
        """ Create a primitive that will delete a node instane. """
        raise NotImplementedError()
    def cri_drop_infrastructure(self, infra_id):
        """ Create a primitive that will delete an infrastructure instance. """
        raise NotImplementedError()

    def cancel_pending(self, deadline=None):
        """
        Registers that commands up to a specific time should be considered
        cancelled.

        :param deadline: The strategy will try to cancel/abort/ignore
            performing the commands that has been created before this time.
        :type deadline: :class:`int`, unix timestamp
        """
        if deadline is None:
            # TODO This default may be a problem. If a command has a timestamp
            # between NOW and NOW+1, it must be performed. But the +1 here
            # may break this (race condition). Must think this through.
            deadline = int(time.time()) + 1 # ~ceil()
        self.cancelled_until = deadline
        self.strategy.cancel_pending()
