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
    def test_sc_create_env(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        nid = uid()
        sc.create_environment(nid)
        self.assertEqual(repr(ib), '%s:[]'%nid)
    def test_sc__node(self):
        ib = DummyInfoBroker()
        sc = DummyServiceComposer(ib)
        nid = uid()
        sc.create_environment(nid)
        self.assertEqual(repr(ib), '%s:[]'%nid)

if __name__ == '__main__':
    unittest.main()
