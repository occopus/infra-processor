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
#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-InfraProcessor',
    version='1.5',
    author='MTA SZTAKI',
    author_email='occopus@lpds.sztaki.hu',
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
        'occo.plugins.infraprocessor.node_resolution.basic',
        'occo.plugins.infraprocessor.node_resolution.cloudinit',
        'occo.plugins.infraprocessor.node_resolution.docker',
    ],
    scripts=[],
    url='https://github.com/occopus',
    license='LICENSE.txt',
    description='Occopus Infrastructure Processor',
    long_description=open('README.txt').read(),
    install_requires=[
        'argparse',
        'Jinja2',
        'mysql-python',
        'python-dateutil',
        'ruamel.yaml',
        'ruamel.ordereddict',
        'OCCO-InfoBroker',
        'OCCO-Util',
    ],
)
