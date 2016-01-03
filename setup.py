# -*- coding: utf-8 -*-

import os
from os import path
from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession
from setuptools.command.install import install
from setuptools.command.develop import develop
from subprocess import call, check_call
import getpass
from uuid import uuid4

def create_user():
    import gpc.gpc
    gpc.gpc.set_config('global', 'user', 'name', getpass.getuser())
    gpc.gpc.set_config('global', 'user', 'id', str(uuid4()))

class CustomInstall(install):

    def run(self):
        install.run(self)
        create_user()

class CustomDevelop(develop):

    def run(self):
        develop.run(self)
        create_user()

install_reqs = parse_requirements('requirements.txt', session=PipSession())
reqs = [str(ir.req) for ir in install_reqs]

PACKAGE_NAME = 'gpc'

setup(
    cmdclass={
        'install': CustomInstall,
        'develop': CustomDevelop
    },
    name='gpc',
    version='0.0.1',
    url='http://friendly-sam.readthedocs.org',
    license='LGPLv3',
    author='Rasmus Einarsson',
    author_email='rasmus.einarsson@sp.se',
    description='Toolbox for optimization-based modelling and simulation.',
    install_requires=reqs,
    packages=['gpc'],
    package_dir={'gpc': 'gpc', 'tests': 'tests'},
    package_data={'gpc': ['resources/**/*']},
    test_suite='tests',
    entry_points='''
        [console_scripts]
        gpc=gpc.cli.main:main_group
    ''',
    extras_require = {
        },
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4'
    ]
    )
