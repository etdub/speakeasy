from __future__ import absolute_import
from setuptools import setup
import sys

import speakeasy

if sys.version_info < (2, 7):
    sys.exit('Sorry, Python < 2.7 is not supported')

setup(
    name='speakeasy',
    version=speakeasy.__version__,
    description='Metrics aggregation server',
    author='Eric Wong',
    install_requires=[
        'pyzmq',
        'ujson',
    ],
    packages=['speakeasy', 'speakeasy.emitter'],
    scripts=['bin/speakeasy'],
    license='Apache License 2.0',
    url='https://github.com/etdub/speakeasy',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: Apache Software License',
    ],
)
