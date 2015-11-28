# -*- coding: utf-8 -*-

import os
from os import path
from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession

here = path.abspath(path.dirname(__file__))

install_reqs = parse_requirements('requirements.txt', session=PipSession())
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='gpc',
    version='0.0.1',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    url='http://friendly-sam.readthedocs.org',
    license='LGPLv3',
    author='Rasmus Einarsson',
    author_email='rasmus.einarsson@sp.se',
    description='Toolbox for optimization-based modelling and simulation.',
    install_requires=reqs,
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
