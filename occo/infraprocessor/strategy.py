#
# Copyright (C) 2014 MTA SZTAKI
#

""" Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__ = ['Strategy', 'SequentialStrategy', 'ParallelProcessesStrategy']

import logging
import occo.util as util
import occo.util.factory as factory
import threading
from occo.exceptions.orchestration import *

log = logging.getLogger('occo.infraprocessor.strategy')

###################################################
# Strategies to process parallelizable instructions
###

class Strategy(factory.MultiBackend):
    """
    Abstract strategy for processing a batch of *independent* commands.
    """

    def cancel_pending(self):
        """
        Registers that performing the batch should be aborted. It only works
        iff the implementation of the strategy supports it (e.g. a thread pool
        or a sequential iteration can be aborted).
        """
        raise NotImplementedError()

    def perform(self, infraprocessor, instruction_list):
        """
        Perform the instruction list. The actual strategy used is defined by
        subclasses.

        :param infraprocessor: The infraprocessor that calls this method.
            Commands are perfomed *on* an infrastructure processor, therefore
            they need a reference to it.
        :param instruction_list: An iterable containing the commands to be
            performed.
        """
        try:
            return self._perform(infraprocessor, instruction_list)
        except KeyboardInterrupt:
            log.debug('Received KeyboardInterrupt; cancelling pending tasks')
            self.cancel_pending()
            raise
        except NodeCreationError as ex:
            # Undoing CreateNode is done here, not inside the CreateNode
            # command, so cancellation can be dispatched (_handle_...) *before*
            # the faulty node is started to be undone. I.e.: the order of the
            # following two lines matters:
            self._handle_infraprocessorerror(infraprocessor, ex)
            self._undo_create_node(infraprocessor, ex.instance_data)
            raise
        except CriticalInfraProcessorError as ex:
            self._handle_infraprocessorerror(infraprocessor, ex)
            raise

    def _undo_create_node(self, infraprocessor, instance_data):
       undo_command = infraprocessor.cri_drop_node(instance_data)
       try:
           undo_command.perform(infraprocessor)
       except Exception:
           # TODO: maybe store instance_data in UDS in case it's stuck?
           log.exception(
               'IGNORING error while dropping partially started node')

    def _handle_infraprocessorerror(self, infraprocessor, ex):
        log.error('Strategy.perform: exception: %s', ex)
        log.error('A critical error (%s) has occured, aborting remaining '
                  'commands in this batch (infra_id: %r).',
                  ex.__class__.__name__, ex.infra_id)
        self.cancel_pending()

    def _perform(self, infraprocessor, instruction_list):
        """
        Core function of :meth:`perform`. This method must be overridden in
        the implementations of the strategy.

        The actual implementation is expected to handle
        :class:`~occo.exceptions.orchestration.MinorInfraProcessorError`\ s by
        itself, but propagate other exceptions upward so :meth:`perform` can
        handle the uniformly.

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
    def __init__(self):
        self.cancelled = False

    def cancel_pending(self):
        self.cancelled = True

    def _perform(self, infraprocessor, instruction_list):
        log.debug('Peforming instructions SEQUENTIALLY: %r',
                  instruction_list)
        results = list()
        for i in instruction_list:
            if self.cancelled:
                break
            try:
                result = i.perform(infraprocessor)
            except MinorInfraProcessorError:
                log.error('A non-critical error has occured, ignoring.')
                results.append(None)
            else:
                results.append(result)
        return results

class PerformThread(threading.Thread):
    """
    Thread object used by :class:`ParallelProcessesStrategy` to perform a
    single command.
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

    .. todo:: Implement using processes instead.

    .. todo:: Must implement :meth:`cancel_pending` also.
    """

    def _perform(self, infraprocessor, instruction_list):
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
