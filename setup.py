#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-InfraProcessor',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=[
        'occo',
        'occo.plugins',
        'occo.plugins.infraprocessor',
        'occo.plugins.infraprocessor.node_resolution',
    ],
    packages=[
        'occo.infraprocessor',
        'occo.infraprocessor.synchronization',
    ],
    py_modules=[
        'occo.infraprocessor.node_resolution',
        'occo.infraprocessor.strategy',
        'occo.infraprocessor.synchronization.primitives',
        'occo.plugins.infraprocessor.basic_infraprocessor',
        'occo.plugins.infraprocessor.node_resolution.chef_cloudinit',
        'occo.plugins.infraprocessor.node_resolution.cloudbroker',
    ],
    scripts=[],
    url='http://www.lpds.sztaki.hu/',
    license='LICENSE.txt',
    description='OCCO Infrastructure Processor',
    long_description=open('README.txt').read(),
    install_requires=[
        'argparse',
        'Jinja2',
        'python-dateutil',
        'PyYAML',
        'OCCO-InfoBroker',
        'OCCO-Util',
    ],
)
