#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.infraprocessor.basic_infraprocessor as bip
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
        self.assertEqual(self.infrap.cri_create_infrastructure(Stuff()).__class__,
                         bip.CreateInfrastructure)
    def test_cmd_2(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_create_node(Stuff()).__class__,
                         bip.CreateNode)
    def test_cmd_3(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_infrastructure(Stuff()).__class__,
                         bip.DropInfrastructure)
    def test_cmd_4(self):
        self.infrap = ip.InfraProcessor.instantiate(
            'basic', self.uds, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_node(Stuff()).__class__,
                         bip.DropNode)
