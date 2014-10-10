#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor as ip
import threading as th
import time

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.ib = DummyInfoBroker()
        self.sc = DummyServiceComposer(self.ib)
        self.ch = DummyCloudHandler(self.ib)
    def test_create_environment(self):
        event = th.Event()
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        skeleton = ip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.RemoteInfraProcessor(cfg.ip_mqconfig)

        eid = uid()
        cmd_cre = stub.cri_create_env(eid)
        with stub:
            stub.push_instructions(cmd_cre)
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
        self.assertEqual(repr(self.ib), '%s:[]'%eid)
    def test_create_node(self):
        event = th.Event()
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        skeleton = ip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.RemoteInfraProcessor(cfg.ip_mqconfig)

        eid = uid()
        node = DummyNode(uid(), eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        with stub:
            stub.push_instructions(cmd_cre)
            stub.push_instructions(cmd_crn)
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
        self.assertEqual(repr(self.ib), '%s:[%s_True]'%(eid, node.id))
    def test_drop_node(self):
        event = th.Event()
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        skeleton = ip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.RemoteInfraProcessor(cfg.ip_mqconfig)

        eid = uid()
        node = DummyNode(uid(), eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        cmd_rmn = infrap.cri_drop_node(node.id)
        with stub:
            stub.push_instructions(cmd_cre)
            stub.push_instructions(cmd_crn)
            stub.push_instructions(cmd_rmn)
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
        self.assertEqual(repr(self.ib), '%s:[]'%eid)
    def test_drop_environment(self):
        event = th.Event()
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        skeleton = ip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.RemoteInfraProcessor(cfg.ip_mqconfig)

        eid = uid()
        node = DummyNode(uid(), eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        cmd_rmn = infrap.cri_drop_node(node.id)
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
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        skeleton = ip.RemoteInfraProcessorSkeleton(
            infrap, cfg.ip_mqconfig, cfg.ctl_mqconfig, event)
        stub = ip.RemoteInfraProcessor(cfg.ip_mqconfig)

        eid = uid()
        nodes = list(DummyNode(uid(), eid) for i in xrange(5))
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
            '{0}:[{1}]'.format(eid, ', '.join('{0}_True'.format(n.id)
                                                for n in nodes)))

if __name__ == '__main__':
    unittest.main()
