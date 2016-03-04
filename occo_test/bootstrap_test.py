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
        sc = DummyConfigManager()
        nid = uid()
        sc.create_infrastructure(nid)
        self.assertEqual(repr(self.ib), '{0}:[]'.format(nid))
    def test_sc_register_node(self):
        sc = DummyConfigManager()
        eid = uid()
        sc.create_infrastructure(eid)
        node = DummyNode(eid, uid())
        sc.register_node(node)
        self.assertEqual(
            repr(self.ib),
            '{0}:[{1}_False]'.format(eid, node['node_id']))
    def test_ch_create_node(self):
        sc = DummyConfigManager()
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
        sc = DummyConfigManager()
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
        sc = DummyConfigManager()
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
        sc = DummyConfigManager()
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
