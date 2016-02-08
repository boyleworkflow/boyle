# -*- coding: utf-8 -*-

import os
import subprocess
import logging
from uuid import uuid4
import hashlib
import json

logger = logging.getLogger(__name__)

class GenericError(Exception): pass


class ConflictException(Exception):
    """docstring for ConflictException"""
    def __init__(self, calc_id, path):
        super(ConflictException, self).__init__()
        self.calc_id = calc_id
        self.path = path


class NotFoundException(Exception): pass


def unique_json(obj):
    return json.dumps(obj, sort_keys=True)

def hexdigest(str_or_unicode):
    return hashlib.sha1(str_or_unicode.encode('utf-8')).hexdigest()

def digest_file(path):
    with open(path, 'rb') as f:
        return hashlib.sha1(f.read()).hexdigest()

def get_calc_id(task, input_digests):
    """
    Compute the calculation id of a task with certain input digests.

    Args:
        task: The Task object in question.
        input_digests (dict-like): A mapping such that
            input_digests[path] == digest for all inputs used by the task.

    Returns:
        A string with the calculation id.

    """
    # Note: calc_id could be constructed in different ways, but
    # must be independent of the order of inputs.
    id_contents = [task.id, {p: input_digests[p] for p in task.inputs}]
    return hexdigest(unique_json(id_contents))

def get_comp_id(calc_id, input_comp_ids):
    """
    Compute the composition id of a calculation and its input compositions.

    Args:
        calc_id: The calculation id of the calculation.
        input_comp_ids (iterable): An iterable with the comp_id values
            of the inputs to the composition.

    Returns:
        A string with the composition id.

    """
    # Note: comp_id could be constructed in different ways, but
    # must be independent of the order of inputs.
    id_contents = [calc_id, list(sorted(input_comp_ids))]
    return hexdigest(unique_json(id_contents))


class Task(object):
    def __init__(self, command):
        super(Task, self).__init__()
        self.command = command
        self.inputs = []
        self.outputs = []
        
    def run(self, workdir):
        original_wd = os.getcwd()
        os.chdir(workdir)
        try:
            subprocess.call(self.command, shell=True)
        except Exception as e:
            raise e
        finally:
            os.chdir(original_wd)

    def __repr__(self):
        return '{} with command "{}"'.format(type(self), self.command)

class ShellTask(Task):
    """docstring for ShellTask"""
    def __init__(self, command, inputs, outputs):
        super(ShellTask, self).__init__(command)
        self.inputs = inputs
        self.outputs = outputs
        self.id = command

class CopyTask(Task):
    def __init__(self, outputs):
        original_wd = os.getcwd()
        files = [original_wd + '/' + o for o in outputs]
        command = "cp " + ' '.join(files) + ' ./'
        super(CopyTask, self).__init__(command)
        self.inputs = []
        self.outputs = outputs
        self.id = self.command
