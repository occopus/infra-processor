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

        except NodeCreationError as ex:
            log.error('A node creation error has occured, aborting remaining '
                      'commands in this batch and undoing partial node '
                      'creation (%r).', ex.instance_data['node_id'])
            self._suspend_infrastructure(ex.infra_id)
            self.cancel_pending(infraprocessor)
            self._undo_create_node(infraprocessor, ex.instance_data)

        except CriticalInfraProcessorError as ex:
            log.error('A critical error has occured, aborting remaining '
                      'commands in this batch.')
            self._suspend_infrastructure(ex.infra_id)
            self.cancel_pending(infraprocessor)

        else:
            return results

    def _undo_create_node(self, infraprocessor, instance_data):
       undo_command = infraprocessor.cri_drop_node(instance_data)
       try:
           undo_command.perform(infraprocessor)
       except Exception:
           log.exception(
               'Error while dropping partially started node; IGNORING:')

    def _suspend_infrastructure(self, infra_id):
        # TODO: implement suspending infrastructure (see OCD-83)
        pass

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
