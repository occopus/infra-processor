#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-InfraProcessor',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=['occo',
                        'occo.plugins',
                        'occo.plugins.infraprocessor',
                        'occo.plugins.infraprocessor.node_resolution',
                        ],
    py_modules=[
                'occo.infraprocessor.basic_infraprocessor',
                'occo.infraprocessor.node_resolution',
                'occo.infraprocessor.strategy',
                'occo.infraprocessor.synchronization.primitives',
                'occo.plugins.infraprocessor.node_resolution.chef_cloudinit',
    ],
    packages=[
        'occo.infraprocessor',
        'occo.infraprocessor.synchronization',
    ],
    scripts=[],
    url='http://www.lpds.sztaki.hu/',
    license='LICENSE.txt',
    description='OCCO Infrastructure Processor',
    long_description=open('README.txt').read(),
    install_requires=['argparse',
                      'PyYAML',
                      'python-dateutil',
                      'Jinja2',
                      'OCCO-Util',
                      'OCCO-InfoBroker'],
)
