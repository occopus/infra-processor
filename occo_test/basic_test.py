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
