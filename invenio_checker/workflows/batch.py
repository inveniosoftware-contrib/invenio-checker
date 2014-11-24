# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2013, 2014 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
import json
from invenio.modules.checker.rules import Rule
from importlib import import_module
from invenio.base.factory import with_app_context
from invenio.modules.workflows.models import BibWorkflowObject

@with_app_context()
def run_batch(obj, eng):
    """TODO: Docstring for run_batch.

    :param obj: ids, common data, Rule in json
    :type  obj: dict
    :param eng: workflow engine
    :type  eng: BibWorkflowEngine

    :returns: TODO
    :rtype:   TODO

    :raises: TODO
    """
    # Load everything from obj
    ids = obj.data['ids']
    common = obj.data['common']
    rule = Rule.from_json(obj.data['rule_json'])

    # Execute pre- function
    plugin_module = import_module(rule.pluginspec)
    obj.data = plugin_module.pre_check()


class batch(object):
    workflow = [run_batch]


__all__ = ['batch']
