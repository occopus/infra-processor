#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor as ip
import threading
import uuid

def uid():
    return str(uuid.uuid4())

class BaseTest(unittest.TestCase):
    def test_sc_create_infrastructure(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        nid = uid()
        sc.create_infrastructure(nid)
        self.assertEqual(repr(ib), '{0}:[]'.format(nid))
    def test_sc_register_node(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        self.assertEqual(
            repr(ib),
            '{0}:[{1}_False]'.format(eid, node['node_id']))
    def test_ch_create_node(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        ch = DummyCloudHandler(ib)
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        self.assertEqual(
            repr(ib),
            '{0}:[{1}_True]'.format(eid, node['node_id']))
    def test_ch_drop_node(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        ch = DummyCloudHandler(ib)
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        self.assertEqual(
            repr(ib),
            '{0}:[{1}_False]'.format(eid, node['node_id']))
    def test_sc_drop_node(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        ch = DummyCloudHandler(ib)
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        sc.drop_node(node)
        self.assertEqual(repr(ib), '{0}:[]'.format(eid))
    def test_sc_drop_infrastructure(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        ch = DummyCloudHandler(ib)
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        ch.create_node(node)
        ch.drop_node(node)
        sc.drop_node(node)
        sc.drop_infrastructure(eid)
        self.assertEqual(repr(ib), '')
