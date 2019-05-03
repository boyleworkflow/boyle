# -*- coding: utf-8 -*-

"""Top-level package for Boyle Workflow."""

__version__ = '0.1.2'

import logging

import boyleworkflow.config
from boyleworkflow.core import Op, Comp, Calc
from boyleworkflow.log import Log, ConflictException, NotFoundException
from boyleworkflow.storage import Storage
from boyleworkflow.task import shell
from boyleworkflow.make import make

logger = logging.getLogger(__name__)
