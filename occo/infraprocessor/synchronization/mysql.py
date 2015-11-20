#
#Copyrigth (C) 2014 MTA SZTAKI
#

"""
Accessory modules for the Wordpress - MySQL OCCO Demo
"""

import occo.util as util
import occo.infobroker as ib
import occo.util.factory as factory
import logging
from occo.infraprocessor.synchronization import NodeSynchStrategy
import MySQLdb

log=logging.getLogger('occo.demo.wp_mysql')

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



