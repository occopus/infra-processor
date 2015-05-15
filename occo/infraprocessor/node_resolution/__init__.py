#
# Copyright (C) 2014 MTA SZTAKI
#

""" Resolution of node definitions.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This module provides utilities to resolve the definition of an infrastructure
node. That is, it uses the abstract definition of the node's type (its
:ref:`implementation <nodedefinition>`) and gathers all the information
necessary to actually create and build such a node on a given backend.

The correct resolution algorithm **depends on** the type of description, which
is a function of the type of the **service composer** and the type of the
**cloud handler** used to instantiate the node. Because of this dependency,
the resolution utilises the :mod:`Factory <occo.util.factory>` pattern to
select the correct :class:`Resolver`.
"""

__import__('pkg_resources').declare_namespace(__name__)
