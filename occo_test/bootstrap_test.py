#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infobroker as ib
import occo.infraprocessor as ip
import threading
import uuid

def uid():
    return str(uuid.uuid4())

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.ib = ib.real_main_info_broker = DummyInfoBroker()
    def test_sc_create_infrastructure(self):
        sc = DummyServiceComposer()
        nid = uid()
        sc.create_infrastructure(nid)
        self.assertEqual(repr(self.ib), '{0}:[]'.format(nid))
    def test_sc_register_node(self):
        sc = DummyServiceComposer()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}_False]'.format(eid, node['node_id']))
    def test_ch_create_node(self):
        sc = DummyServiceComposer()
        ch = DummyCloudHandler()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}_True]'.format(eid, node['node_id']))
    def test_ch_drop_node(self):
        sc = DummyServiceComposer()
        ch = DummyCloudHandler()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}_False]'.format(eid, node['node_id']))
    def test_sc_drop_node(self):
        sc = DummyServiceComposer()
        ch = DummyCloudHandler()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        sc.drop_node(node)
        self.assertEqual(repr(self.ib), '{0}:[]'.format(eid))
    def test_sc_drop_infrastructure(self):
        sc = DummyServiceComposer()
        ch = DummyCloudHandler()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        sc.drop_node(node)
        sc.drop_infrastructure(eid)
        self.assertEqual(repr(self.ib), '')
