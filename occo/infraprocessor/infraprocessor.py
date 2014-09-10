#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['InfraProcessor']

import occo.util as util
import occo.util.communication as comm

class InfraProcessor(object):
    def __init__(self, infobroker, cloudhandler):
        self.ib = infobroker
        self.cloudhandler = cloudhandler
