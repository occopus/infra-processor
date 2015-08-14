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
import multiprocessing
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
        self.cancelled = False
        log.debug('Peforming instructions SEQUENTIALLY: %r',
                  instruction_list)

        results = list()
        for i in instruction_list:
            if self.cancelled:
                break

            try:
                result = i.perform(infraprocessor)
            except MinorInfraProcessorError as ex:
                log.error('IGNORING non-critical error: %s', ex)
                results.append(None)
            else:
                results.append(result)
        return results

class PerformProcess(multiprocessing.Process):
    """
    Process object used by :class:`ParallelProcessesStrategy` to perform a
    single command.
    """
    def __init__(self, procname, infraprocessor, instruction):
        super(PerformProcess, self).__init__(name=procname,target=self.run)
        self.infraprocessor = infraprocessor
        self.instruction = instruction

    def run(self):
        try:
            return self.instruction.perform(self.infraprocessor)
        except BaseException:
            log.exception("Unhandled exception in process:")

@factory.register(Strategy, 'parallel')
class ParallelProcessesStrategy(Strategy):
    """
    Implements :class:`Strategy`, performing the commands in a parallel manner.

    .. todo:: Must implement :meth:`cancel_pending` also.
    """

    def _perform(self, infraprocessor, instruction_list):
        processes=list()
        for ind, i in enumerate(instruction_list):

            def f():
                yield getattr(i, 'infra_id', None)
                yield getattr(i, 'instance_data', dict()).get('node_id')
                yield getattr(i, 'node_description', dict()).get('name')
                yield 'noID'

            strid = util.icoalesce(f())
            processes.append(
                    PerformProcess(
                        'Proc{0}-{1}'.format(i.__class__.__name__,strid),
                        infraprocessor, 
                        i))
        # Start all processes
        for p in processes:
            log.debug('Starting process for %r', p.instruction)
            p.start()
            log.debug('STARTED process for %r', p.instruction)
        results = list()
        # Wait for results
        for p in processes:
            log.debug('Waiting for process %r', p.instruction)
            p.join()
            log.debug('FINISHED Process for %r', p.instruction)
            #log.debug('RESULT received %r', p.result)
            #results.append(p.result)
