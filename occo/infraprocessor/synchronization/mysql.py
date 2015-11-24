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

"""
Synchronization module for mysql database(s)
.. moduleauthor:: Adam Novak <novak.adam@sztaki.mta.hu>
"""

import occo.util as util
import occo.infobroker as ib
import occo.util.factory as factory
import logging
from occo.infraprocessor.synchronization import NodeSynchStrategy

log=logging.getLogger('occo.infraprocessor.synchronization')

@factory.register(NodeSynchStrategy, 'mysql_server')
class MysqlServerSynchStragegy(NodeSynchStrategy):
    def get_node_address(self, infra_id, node_id):
        return ib.main_info_broker.get('node.address',
                                       infra_id=infra_id, node_id=node_id)
    def get_kwargs(self):
        if not hasattr(self, 'kwargs'):
            self.kwargs = self.resolved_node_definition.get(
                'synch_strategy', dict())
            if isinstance(self.kwargs, basestring):
                self.kwargs = dict()
        return self.kwargs
    
    def is_ready(self):
    """
    Method for checking mysql database availability.
    """
        import MySQLdb
        host = self.get_node_address(self.infra_id, self.node_id)
        if not ib.main_info_broker.get('synch.node_reachable',
                                       infra_id = self.infra_id,
                                       node_id = self.node_id):
            return False
        try:
            dblist = self.get_kwargs().get('databases', list())
            log.debug('Checking mysql database availability:')
            for db in dblist:
                conn = MySQLdb.connect(
                    host, db.get('user'), db.get('pass'),
                    db.get('name'))
                conn.close()
            log.debug('Connection successful')
        except MySQLdb.Error as e:
            log.debug('Connecton failed: %s',e)
            return False
        return True



