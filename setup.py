#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-InfraProcessor',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=['occo',
                        'occo.infraprocessor',
                        'occo.infraprocessor.node_resolution',
                        'occo.infraprocessor.node_resolution.backends'],
    py_modules=['occo.infraprocessor.infraprocessor',
                'occo.infraprocessor.basic_infraprocessor',
                'occo.infraprocessor.remote_infraprocessor',
                'occo.infraprocessor.strategy',
                'occo.infraprocessor.node_resolution.resolution',
                'occo.infraprocessor.node_resolution.backends.chef'],
    packages=['occo.infraprocessor'],
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
