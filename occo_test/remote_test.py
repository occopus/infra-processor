#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor.basic_infraprocessor
import occo.infraprocessor.remote_infraprocessor as rip
import occo.infraprocessor.infraprocessor as ip
from occo.infobroker.uds import UDS
import occo.util as util
import threading as th
import time

@unittest.skip
class RemoteTest(unittest.TestCase):
    def setUp(self):
        self.ib = DummyInfoBroker(main_info_broker=True)
        self.sc = DummyServiceComposer(self.ib)
        self.ch = DummyCloudHandler(self.ib)
        self.uds = UDS.instantiate(protocol='dict')

    def test_process(self):
        event = th.Event()
        infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        skeleton = rip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)

        eid, nid = uid(), uid()
        node = DummyNode(eid, nid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        cmd_rmn = infrap.cri_drop_node(node['node_id'])
        cmd_rme = infrap.cri_drop_environment(eid)
        with stub:
            stub.push_instructions(cmd_cre)
            stub.push_instructions(cmd_crn)
            stub.push_instructions(cmd_rmn)
            stub.push_instructions(cmd_rme)
        def cth():
            try:
                with skeleton:
                    skeleton.start_consuming()
            except Exception:
                log.exception('Exception in consumer thread')
        consumer = th.Thread(target=cth)
        consumer.start()
        time.sleep(1)
        event.set()
        self.assertEqual(repr(self.ib), '')

    def test_create_multiple_nodes(self):
        event = th.Event()
        infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        skeleton = rip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)

        eid = uid()
        nodes = list(DummyNode(eid, uid()) for i in xrange(5))
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crns = (infrap.cri_create_node(node) for node in nodes)
        with stub:
            stub.push_instructions(cmd_cre)
            stub.push_instructions(cmd_crns)
        def cth():
            try:
                with skeleton:
                    skeleton.start_consuming()
            except Exception:
                log.exception('Exception in consumer thread')
        consumer = th.Thread(target=cth)
        consumer.start()
        time.sleep(1)
        event.set()
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}]'.format(eid, ', '.join('{0}_True'.format(n['node_id'])
                                                for n in nodes)))
