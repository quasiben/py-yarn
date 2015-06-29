#!/usr/bin/env python

import os
from distutils.core import setup


setup(
    name="py-yarn",
    version='0.1.0',
    packages=['yarn.protobuf', 'yarn.rpc'],
    author='Avishai Ish-Shalom',
    author_email='avishai@fewbytes.com',
    license='Apache V2',
    keywords='yarn hadoop',
    description='A Hadoop Yarn API client',
    install_requires=['snakebite',
                      'click'],
    scripts=[os.path.join('bin', 'yarn-client',)],
)
