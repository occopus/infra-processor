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
        self.infrap = ip.InfraProcessor(self.ib, self.ch, self.sc)
    def test_cmd_1(self):
        self.assertEqual(self.infrap.cri_create_env(None).__class__,
                         ip.InfraProcessor.CreateEnvironment)

if __name__ == '__main__':
    unittest.main()
