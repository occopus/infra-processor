#
# Copyright (C) 2014 MTA SZTAKI
#
# Unit tests for the SZTAKI Cloud Orchestrator
#

import unittest
from common import *
import occo.plugins.infraprocessor.basic_infraprocessor as bip
import occo.infraprocessor as ip
import occo.util as util
import occo.infobroker as ib
from occo.infobroker.uds import UDS
from occo.infobroker.kvstore import KeyValueStore
import threading

class Stuff(): pass

class BaseTest(unittest.TestCase):
    def setUp(self):
        ib.set_all_singletons(
            DummyInfoBroker(),
            UDS.instantiate(protocol='dict'),
            None,
            DummyCloudHandler(),
            DummyServiceComposer(),
        )
    def test_cmd_1(self):
        self.infrap = ip.InfraProcessor.instantiate('basic')
        self.assertEqual(self.infrap.cri_create_infrastructure(Stuff()).__class__,
                         bip.CreateInfrastructure)
    def test_cmd_2(self):
        self.infrap = ip.InfraProcessor.instantiate('basic')
        self.assertEqual(self.infrap.cri_create_node(Stuff()).__class__,
                         bip.CreateNode)
    def test_cmd_3(self):
        self.infrap = ip.InfraProcessor.instantiate('basic')
        self.assertEqual(self.infrap.cri_drop_infrastructure(Stuff()).__class__,
                         bip.DropInfrastructure)
    def test_cmd_4(self):
        self.infrap = ip.InfraProcessor.instantiate('basic')
        self.assertEqual(self.infrap.cri_drop_node(Stuff()).__class__,
                         bip.DropNode)
