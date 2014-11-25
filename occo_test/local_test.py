#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor as ip
import threading

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.ib = DummyInfoBroker()
        self.sc = DummyServiceComposer(self.ib)
        self.ch = DummyCloudHandler(self.ib)
    def test_create_environment(self):
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        eid = uid()
        cmd = infrap.cri_create_env(eid)
        infrap.push_instructions(cmd)
        self.assertEqual(repr(self.ib), '%s:[]'%eid)
    def test_create_node(self):
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        eid = uid()
        node = DummyNode(eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        infrap.push_instructions(cmd_cre)
        infrap.push_instructions(cmd_crn)
        self.assertEqual(repr(self.ib), '%s:[%s_True]'%(eid, node.id))
    def test_drop_node(self):
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        eid = uid()
        node = DummyNode(eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        infrap.push_instructions(cmd_cre)
        infrap.push_instructions(cmd_crn)
        cmd_rmn = infrap.cri_drop_node(node.id)
        infrap.push_instructions(cmd_rmn)
        self.assertEqual(repr(self.ib), '%s:[]'%eid)
    def test_drop_environment(self):
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        eid = uid()
        node = DummyNode(eid)
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crn = infrap.cri_create_node(node)
        infrap.push_instructions(cmd_cre)
        infrap.push_instructions(cmd_crn)
        cmd_rmn = infrap.cri_drop_node(node.id)
        cmd_rme = infrap.cri_drop_environment(eid)
        infrap.push_instructions(cmd_rmn)
        infrap.push_instructions(cmd_rme)
        self.assertEqual(repr(self.ib), '')
    def test_create_multiple_nodes(self):
        infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        eid = uid()
        nodes = list(DummyNode(eid) for i in xrange(5))
        cmd_cre = infrap.cri_create_env(eid)
        cmd_crns = (infrap.cri_create_node(node) for node in nodes)
        infrap.push_instructions(cmd_cre)
        infrap.push_instructions(cmd_crns)
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}]'.format(eid, ', '.join('{0}_True'.format(n.id)
                                                for n in nodes)))
