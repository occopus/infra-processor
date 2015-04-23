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

log = logging.getLogger('occo.infraprocessor.strategy')

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
