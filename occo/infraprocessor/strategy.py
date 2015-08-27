#
# Copyright (C) 2014 MTA SZTAKI
#

""" Infrastructure Processor for OCCO

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

"""

__all__ = ['Strategy', 'SequentialStrategy', 'ParallelProcessesStrategy']

import logging
import os, signal
import sys, traceback
import occo.util as util
import occo.util.factory as factory
import multiprocessing
from occo.exceptions.orchestration import *

log = logging.getLogger('occo.infraprocessor.strategy')
datalog = logging.getLogger('occo.data.infraprocessor.strategy')

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
    def __init__(self, procid, procname, infraprocessor, instruction,
                 result_queue):
        super(PerformProcess, self).__init__(name=procname,target=self.run)
        self.infraprocessor = infraprocessor
        self.instruction = instruction
        self.result_queue = result_queue
        self.procid = procid
        self.log = logging.getLogger('occo.infraprocessor.strategy.subprocess')

    def return_result(self, result):
        self.result_queue.put((self.procid, result, None))

    def return_exception(self, exc_info):
        error = {
            'type'  : exc_info[0],
            'value' : exc_info[1],
            # Raw traceback cannot be passed through Queue
            'tbstr' : ''.join(traceback.format_tb(exc_info[2])),
        }
        self.log.debug('Returning exception: %r', error)
        self.result_queue.put((self.procid, None, error))

    def run(self):
        try:
            self.return_result(self.instruction.perform(self.infraprocessor))
        except KeyboardInterrupt:
            log.debug('Operation cancelled.')
            self.return_result(None)
        except Exception:
            log.exception("Unhandled exception in process:")
            exc = sys.exc_info()
            self.return_exception(exc)

@factory.register(Strategy, 'parallel')
class ParallelProcessesStrategy(Strategy):
    """
    Implements :class:`Strategy`, performing the commands in a parallel manner.
    """

    def _possible_process_names(self, instr):
        """
        Lists the possible ids that can be used in the name of a process.
        """
        yield getattr(instr, 'infra_id', None)
        yield getattr(instr, 'instance_data', dict()).get('node_id')
        yield getattr(instr, 'node_description', dict()).get('name')
        yield 'noID'

    def _mk_process_name(self, instr):
        """
        Generate a process name based on the instruction
        """
        return 'Proc{0}-{1}'.format(
            instr.__class__.__name__,
            util.icoalesce(self._possible_process_names(instr))
        )

    def _add_process(self, instruction):
        """
        Generate a process object for this instruction and append it to the
        existing set of processes.
        """
        index = len(self.results)
        self.results.append(None)
        self.processes[index] = \
            PerformProcess(
                index, self._mk_process_name(instruction),
                self.infraprocessor, instruction, self.result_queue)

    def _generate_processes(self, instruction_list):
        """
        Generate the list of :class:`multiprocessing.Process` objects to be
        started.
        """
        assert not getattr(self, 'processes', None)
        self.results = list()
        self.processes = dict()
        for instruction in instruction_list:
            self._add_process(instruction)

    def _process_one_result(self):
        """
        Wait and then process a sub-process result.
        """
        log.debug('Waiting for a sub-process to finish...')
        procid, result, error = self.result_queue.get()
        log.debug('Result for process %r has arrived: %r',
                  self.processes[procid].name, result or error)

        del self.processes[procid]

        if error:
            log.debug('Exception occured in sub-process:\n%s\n%r',
                      error['tbstr'], error['value'])
            raise error['type'], error['value']
        else:
            self.results[procid] = result

    def _perform(self, infraprocessor, instruction_list):
        self.infraprocessor = infraprocessor
        self.result_queue = multiprocessing.Queue()
        self._generate_processes(instruction_list)

        # Start all processes
        for p in self.processes.itervalues():
            log.debug('Starting process for %r', p.instruction)
            p.start()
            log.debug('STARTED process for %r', p.instruction)

        # Wait for results
        log.debug('Waiting for sub-processes to finish')
        while self.processes:
            try:
                self._process_one_result()
            except NodeCreationError as ex:
                # Undoing CreateNode is done here, not inside the CreateNode
                # command, so cancellation can be dispatched (_handle_...)
                # *before* the faulty node is started to be undone. I.e.: the
                # order of the following two lines matters:
                self.cancel_pending(ex.instance_data)
                raise
            except MinorInfraProcessorError as ex:
                log.debug('IGNORING Minor IP error: %r', ex)
            except CriticalInfraProcessorError as ex:
                log.exception('Caught critical IP error:')
                self.cancel_pending()

        log.debug('All sub-processes finished; exiting.')
        datalog.debug('Sub-process results: %r', self.results)
        return self.results

    def cancel_pending(self, undo_instance_data=None):
        log.debug('Cancelling pending sub-processes')

        for p in self.processes.itervalues():
            try:
                log.debug('Sending SIGINT to %r', p.name)
                os.kill(p.pid, signal.SIGINT)
            except:
                log.exception('IGNORING exception while sending signal:')

        if undo_instance_data:
            log.debug('Undoing create node for %r', instance_data['node_id'])
            undo_command = self.infraprocessor.cri_drop_node(instance_data)
            self._add_process(undo_command)

        for p in self.processes.itervalues():
            log.debug('Waiting for %r to finish', p.name)
            p.join()

        self.processes = None
