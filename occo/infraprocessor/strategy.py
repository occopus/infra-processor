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

"""

__all__ = ['Strategy', 'SequentialStrategy', 'ParallelProcessesStrategy']

from ruamel import yaml
import logging
import os, signal
import sys, traceback
import occo.util as util
import occo.util.factory as factory
import multiprocessing
from occo.exceptions.orchestration import *

log = logging.getLogger('occo.infraprocessor.strategy')
datalog = logging.getLogger('occo.data.infraprocessor.strategy')
clean = util.Cleaner(['resolved_node_definition', 'node_description']).deep_copy

class Strategy(factory.MultiBackend):
    """
    Abstract strategy for processing a batch of *independent* commands.
    """

    def cancel_pending(self, reason=None):
        """
        Registers that performing the batch should be aborted. It only works
        iff the implementation of the strategy supports it (e.g. a thread pool
        or a sequential iteration can be aborted).

        :param Exception reason: Optional. If specified, and it's type is
            :exc:`~occo.exceptions.orchestration.NodeCreationError`, the
            ``instance_data`` in this exception will be used to drop the
            partially created node along with pending ones. This is needed for
            optimization: parallelized strategies can include the implied
            DropNode command in their process pool as they see fit (not
            strictly before/after).

            (Or, in the future, implementations may make more sophisticated
            decisions based on ``reason``.)
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
            raise
        except CriticalInfraProcessorError as ex:
            self._handle_infraprocessorerror(infraprocessor, ex)
            raise

    def _handle_infraprocessorerror(self, infraprocessor, ex):
        log.error('Strategy.perform: exception: %s', ex)
        log.error('A critical error (%s) has occured, aborting remaining '
                  'commands in this batch (infra_id: %r).',
                  ex.__class__.__name__, ex.infra_id)
        self.cancel_pending(ex)

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

    def cancel_pending(self, reason=None):
        if isinstance(reason, NodeCreationError):
            inst_data = reason.instance_data
            log.debug('Undoing create node for %r', inst_data['node_id'])
            undo_command = self.infraprocessor.cri_drop_node(inst_data)
            try:
                undo_command.perform(self.infraprocessor)
            except NodeCreationError as nce:
                log.debug(nce)
            except Exception as ex:
                # TODO: maybe store instance_data in UDS in case it's stuck?
                log.debug(
                    'IGNORING error while dropping partially started node: %r',str(ex))

        self.cancelled = True

    def _perform(self, infraprocessor, instruction_list):
        self.infraprocessor = infraprocessor
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
                log.debug('IGNORING non-critical error: %s', ex)
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
        self.datalog = logging.getLogger('occo.data.infraprocessor.strategy.subprocess')

    def return_result(self, result):
        self.log.debug('Sub-process finished normally; exiting.')
        self.datalog.debug('Returning result: %r', clean(result))
        self.result_queue.put((self.procid, result, None))

    def return_exception(self, exc_info):
        err_type, err_value, err_tbstr = exc_info[0], None, None
        try:
            err_value = yaml.dump(exc_info[1])
        except KeyboardInterrupt, Exception:
            pass
        try:
            err_tbstr = ''.join(traceback.format_tb(exc_info[2]))
        except KeyboardInterrupt, Exception:
            pass
        error = {
            'type'  : err_type,
            'value' : err_value,
            'tbstr' : err_tbstr,
        }
        self.log.debug('Sub-process execution failed: %r', exc_info[1])
        self.result_queue.put((self.procid, None, error))

    def run(self):
        try:
            ret = self.instruction.perform(self.infraprocessor)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            self.return_result(ret)
        except KeyboardInterrupt:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            self.log.debug('Operation cancelled.')
            self.return_result(None)
        except Exception:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
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
        process = \
            PerformProcess(
                index, self._mk_process_name(instruction),
                self.infraprocessor, instruction, self.result_queue)
        self.processes[index] = process
        return process

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
        log.debug('Result for process %r has arrived',
                  self.processes[procid].name)

        del self.processes[procid]

        if error:
            #log.debug("Exception value in sub-process: %s",error['value'])
            error['value'] = yaml.load(error['value'], Loader=yaml.Loader)
            log.debug('Exception occured in sub-process:\n%s\n%r',
                      error['tbstr'], clean(error['value']))
            log.debug('Re-raising the following exception: %s with content: %s',
                      error['type'],error['value'])
            raise error['type'], error['value']
        else:
            self.results[procid] = result

    def _perform(self, infraprocessor, instruction_list):
        self.infraprocessor = infraprocessor
        self.result_queue = multiprocessing.Queue()
        self._generate_processes(instruction_list)

        # Start all processes
        log.debug('Starting sub-process')
        for p in self.processes.itervalues():
            log.debug('Starting sub-process for %r', p.instruction)
            p.start()

        # Wait for results
        log.debug('Waiting for sub-processes to finish')
        while self.processes:
            try:
                self._process_one_result()
            except MinorInfraProcessorError as ex:
                log.debug('IGNORING Minor IP error: %r', ex)

        log.debug('All sub-processes finished; exiting.')
        datalog.debug('Sub-process results: %r', self.results)
        return self.results

    def cancel_pending(self, reason=None):
        log.debug('Cancelling pending sub-processes')

        for p in self.processes.itervalues():
            try:
                log.debug('Sending SIGINT to %r', p.name)
                os.kill(p.pid, signal.SIGINT)
            except Exception as ex:
                log.debug('IGNORING exception while sending signal: %r',str(ex))

        if isinstance(reason, NodeCreationError) and 'instance_id' in reason.instance_data:
            inst_data = reason.instance_data
            log.debug('Undoing create node for %r', inst_data['node_id'])
            undo_command = self.infraprocessor.cri_drop_node(inst_data)
            self._add_process(undo_command).start()

        log.debug('Waiting for sub-processes to finish')
        while self.processes:
            try:
                self._process_one_result()
            except KeyboardInterrupt:
                log.info('Received Ctrl+C while waiting for sub-processes '
                         'to exit. Aborting.')
                raise
            except Exception as ex:
                log.debug(
                    'IGNORING exception while waiting for sub-processes: %r',str(ex))

