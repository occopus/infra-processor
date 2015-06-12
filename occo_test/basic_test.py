#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor.basic_infraprocessor
import occo.infraprocessor.infraprocessor as ip
import occo.util as util
from occo.infobroker.uds import UDS
from occo.infobroker.kvstore import KeyValueStore
import threading

class Stuff(): pass

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.ib = DummyInfoBroker(main_info_broker=True)
        self.uds = UDS.instantiate(protocol='dict')
        self.sc = DummyServiceComposer(self.ib)
        self.ch = DummyCloudHandler(self.ib)
    def test_cmd_1(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_create_env(Stuff()).__class__,
                         ip.CreateEnvironment)
    def test_cmd_2(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_create_node(Stuff()).__class__,
                         ip.CreateNode)
    def test_cmd_3(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_environment(Stuff()).__class__,
                         ip.DropEnvironment)
    def test_cmd_4(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_node(Stuff()).__class__,
                         ip.DropNode)
    def test_remote_cmd_1(self):
        self.infrap = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)
        self.assertEqual(self.infrap.cri_create_env(Stuff()).__class__,
                         ip.CreateEnvironment)
    def test_remote_cmd_2(self):
        self.infrap = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)
        self.assertEqual(self.infrap.cri_create_node(Stuff()).__class__,
                         ip.CreateNode)
    def test_remote_cmd_3(self):
        self.infrap = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)
        self.assertEqual(self.infrap.cri_drop_environment(Stuff()).__class__,
                         ip.DropEnvironment)
    def test_remote_cmd_4(self):
        self.infrap = ip.InfraProcessor.instantiate('remote', cfg.ip_mqconfig)
        self.assertEqual(self.infrap.cri_drop_node(Stuff()).__class__,
                         ip.DropNode)
