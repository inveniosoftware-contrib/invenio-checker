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

"""Workflow tasks for checker module."""

from functools import wraps, partial
from importlib import import_module

from invenio.modules.checker.rules import Rules, Rule
from invenio.modules.records.api import get_record


def ruledicts(rule_type):
    def _wrapped(obj, eng):
        """Get a list of rules of a specific type.

        :returns: rules
        :rtype:   list of dict

        :raises: AssertionError
        """
        assert rule_type in ("simple", "batch")

        extra_data = obj.get_extra_data()
        rules = Rules.from_jsons(extra_data['rule_jsons'])
        rule_iter = {
            "simple": rules.itersimple,
            "batch": rules.iterbatch
        }

        batch_objects = []
        for rule in rule_iter[rule_type]():
            batch_objects.append(dict(rule))
        return batch_objects
    return _wrapped


def run_batch(obj, eng):
    """Run the batch function of a rule tied to a batch plugin.

    :returns: TODO
    :rtype:   TODO

    :raises: TODO
    """
    # Load everything from obj
    extra_data = obj.get_extra_data()
    recids = extra_data['recids']
    # common = orig_data['common']
    rule = Rule(obj.data)

    # Execute pre- function and save result in extra_data
    plugin_module = import_module(rule.pluginspec)
    records = (get_record(recid) for recid in recids)
    extra_data = obj.get_extra_data()
    if 'result_pre_check' not in extra_data:
        extra_data['result_pre_check'] = {}
    extra_data['result_pre_check'][rule['name']] = plugin_module.pre_check(records)
    obj.extra_data = extra_data


def wf_recids():
    @wraps(wf_recids)
    def _wrapped(obj, eng):
        """Get record IDs related to this workflow."""
        extra_data = obj.get_extra_data()
        return extra_data['recids']
    return _wrapped


def run_check(obj, eng):
    """Check a single record against a rule.

    :returns: TODO
    :rtype:   TODO

    :raises: TODO
    """
    extra_data = obj.get_extra_data()
    # common = extra_data['common']

    # Load record
    recid = obj.data
    record = get_record(recid)

    # Initialize partial
    rule = Rule(extra_data["rule_object"])
    plugin_module = import_module(rule.pluginspec)
    check_record = partial(plugin_module.check_record, record)
    # Append result_pre_check if this is part of a batch plugin
    try:
        result_pre_check = extra_data['result_pre_check'][rule['name']]
    except KeyError:
        pass
    else:
        check_record = partial(check_record, result_pre_check)
    # Append any arguments
    try:
        check_record = partial(check_record, **rule['arguments'])
    except KeyError:
        pass
    # Run
    check_record()
