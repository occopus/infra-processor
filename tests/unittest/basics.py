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
        self.sc = DummyServiceComposer()
        self.ch = DummyCloudHandler()
    def test_cmd_1(self):
        self.infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_create_env(None).__class__,
                         ip.InfraProcessor.CreateEnvironment)
    def test_cmd_2(self):
        self.infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_create_node(None).__class__,
                         ip.InfraProcessor.CreateNode)
    def test_cmd_3(self):
        self.infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_environment(None).__class__,
                         ip.InfraProcessor.DropEnvironment)
    def test_cmd_4(self):
        self.infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
        self.assertEqual(self.infrap.cri_drop_node(None).__class__,
                         ip.InfraProcessor.DropNode)

if __name__ == '__main__':
    unittest.main()
