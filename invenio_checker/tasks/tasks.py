# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2015 CERN.
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

import os
import time

import tempfile
from datetime import datetime
from dictdiffer import diff
from functools import wraps, partial
from importlib import import_module
from invenio.base.globals import cfg

from invenio.ext.sqlalchemy import db
from invenio.legacy.bibsched.bibtask import task_low_level_submission
from invenio.modules.checker.models import CheckerRecord
from invenio.modules.checker.record import AmendableRecord
from invenio.modules.checker.rules import Rules, Rule
from invenio.modules.records.api import get_record, Record

def _partial_argc(func):
    """Argument count of nested partial functions.

    :param func: partial
    :type  func: function

    :returns: argument count
    :rtype:   int
    """
    argc = 0  # Until challenged
    inner_func = None  # So that we don't destruct func
    while True:
        try:
            if inner_func:
                argc += len(inner_func.args)
            else:
                argc += len(func.args)
        except AttributeError:
            pass
        try:
            if inner_func:
                inner_func = inner_func.func
            else:
                inner_func = func.func
        except AttributeError:
            break
    return argc


def _set_done(obj, eng, rule_names, recids):
    # FIXME: Don't re-get extra data. It was already pulled out before this
    # funciton call
    extra_data = obj.get_extra_data()
    now = datetime.now()
    for recid in recids:
        record = _load_record(extra_data, recid)
        expecting_modification = _record_has_changed(obj, eng, record, extra_data)
        db.session.query(CheckerRecord).filter(
            db.and_(
                CheckerRecord.id_bibrec == recids,
                CheckerRecord.name_checker_rule.in_((rule_names))
            )
        ).update(
            {
                "last_run": now,
                "expecting_modification": expecting_modification
            },
            synchronize_session=False)
        db.session.commit()


def _ensure_key(key, dict_):
    if key not in dict_:
        dict_[key] = {}


def _record_has_changed(obj, eng, record, extra_data):
    """Check whether a record is different from its state in the database.

    :param record: record
    :type  record: invenio.modules.records.api.Record or AmendableRecord

    :returns: whether the record has changed
    :rtype:   bool
    """
    recid = record['recid']
    modified_record = record.dumps()
    db_record = get_record(recid).dumps()

    def log_changes(changes):
        """Log the changes done to this record by the last check."""
        try:
            rule_name = record.check.rule_name
        except AttributeError:
            # Not an AmendableRecord, not running a check
            pass
        else:
            obj.log.info(
                "{rule} made the following changes on record {recid}: {changes}"
                .format(rule=rule_name, recid=recid, changes=changes))

    # Try against `extra_data`
    try:
        extra_data_record = extra_data['modified_records'][recid]
    except KeyError:
        # We have not previously stored this record
        pass
    else:
        changes = tuple(diff(extra_data_record, modified_record))
        if changes:
            log_changes(changes)
            return True

    # Try against the database
    changes = tuple(diff(db_record, modified_record))
    if changes:
        log_changes(changes)
        return True

    return False


def _store_extras(obj, eng, extra_data, records):
    """Update `extra_data`, with its new value and the new state of records."""
    for record in records:
        if _record_has_changed(obj, eng, record, extra_data):
            recid = int(record['recid'])
            _ensure_key('modified_records', extra_data)
            extra_data['modified_records'][recid] = record.dumps()
    obj.extra_data = extra_data


def _load_record(extra_data, recid, local_storage_only=False, rule=None):
    """Load a record by recid.

    Revives a record from `extra_data`. If the spell fails, loads it from the
    database.

    :param recid: record ID to load
    :type recid: int

    :param local_storage_only: only return the record if it was found in the
        `extra_data` of the workflow
    :type local_storage_only: bool

    :param rule: name of the running rule. if this is given, AmendableRecord is
        returned instead of Record for consumption in a check
    :type rule: str

    :returns: record
    :rtype: invenio.modules.records.api.Record
    """
    try:
        # Get from extra data.
        record_data = extra_data['modified_records'][recid]
    except KeyError as e:
        # Not found. Get from the database.
        if local_storage_only:
            e.args += ('Non-loaded record {id_} requested. Programming error(?)'
                       .format(id_=recid),)
            raise e
        record_data = get_record(recid).dumps()
        if not record_data:
            raise LookupError('Record {} vanished from the databse!'
                              .format(recid))
    if rule:
        record = AmendableRecord(record_data, rule)
    else:
        record = Record(record_data)
    return record


def set_done(obj, eng):
    """Get a list of rules of a specific type.

    :param rule_type: 'batch' or 'simple'
    :type rule_type: str

    :returns: rules
    :rtype:   list of dict
    """
    extra_data = obj.get_extra_data()
    rule_name = extra_data['rule_object']['name']
    recid = extra_data['record_id']
    record = _load_record(extra_data, recid)
    _set_done(obj, eng, [rule_name], [recid])


def ruledicts(rule_type):
    """Get a list of rules of a specific type.

    :param rule_type: 'batch' or 'simple'
    :type rule_type: str

    :returns: rules
    :rtype:   list of dict
    """
    @wraps(ruledicts)
    def _wrapped(obj, eng):
        extra_data = obj.get_extra_data()
        rules = Rules.from_jsons(extra_data['rule_jsons'])
        rule_iter = {
            "simple": rules.itersimple,
            "batch": rules.iterbatch
        }[rule_type]
        return tuple((dict(rule) for rule in rule_iter()))
    return _wrapped


def wf_recids():
    """Get record IDs related to this workflow."""
    @wraps(wf_recids)
    def _wrapped(obj, eng):
        return obj.get_extra_data()['recids']
    return _wrapped


def run_batch(obj, eng):
    """Run the batch function of a rule tied to a batch plugin.

    Sets `result_pre_check` in extra_data.

    The following function signatures are supported:

    pre_check(records)
    """
    # Load everything
    extra_data = obj.get_extra_data()
    recids = extra_data['recids']
    rule = Rule(extra_data['rule_object'])
    records = (_load_record(extra_data, recid, rule=rule['name'])
               for recid in recids)
    plugin_module = import_module(rule.pluginspec)

    # Execute pre- function and save retval
    _ensure_key('result_pre_check', extra_data)
    extra_data['result_pre_check'][rule['name']] = \
        plugin_module.pre_check(records)
    _store_extras(obj, eng, extra_data, records)


def run_check(obj, eng):
    """Check a single record against a rule.

    The following function signatures are supported:

    check_record(record, **kwargs)
    check_record(record, result_pre_check, **kwargs)
    """
    # Load everything
    extra_data = obj.get_extra_data()
    recid = obj.data
    rule = Rule(extra_data['rule_object'])
    record = _load_record(extra_data, recid, rule=rule['name'])
    plugin_module = import_module(rule.pluginspec)

    # Initialize partial
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
    # Run the check
    try:
        check_record()
    except TypeError as e:
        # Give more details if the reason TypeError was raised was function
        # signature incompatibility (programming error).
        expected_argcount = _partial_argc(check_record)
        given_argcount = plugin_module.check_record.func_code.co_argcount
        if expected_argcount != given_argcount:
            e.args += ('Wrong plugin function signature at {code}'
                       .format(code=plugin_module.check_record.func_code),)
            # FIXME: All future calls to this specific `check_record` are doomed
            # to fail. Either set some variable to stop future `run_checks`
            # from running or do this earlier.
            # FIXME: Same goes for batch, but with greater impact.
        raise
    else:
        _store_extras(obj, eng, extra_data, [record])


def save_records():
    """Upload all modified records."""
    @wraps(save_records)
    def _upload_amendments(obj, eng, holdingpen=False):
        # Load everything
        extra_data = obj.get_extra_data()
        _ensure_key('modified_records', extra_data)
        modified_records = extra_data['modified_records']
        upload = extra_data['common']['upload']
        tickets = extra_data['common']['tickets']
        queue = extra_data['common']['queue']

        modified_records = (Record(r) for r in modified_records.values())
        records_xml = (
            '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'
            '{}'
            '</collection>'
            .format("".join((record.legacy_export_as_marc()
                             for record in modified_records)))
        )

        # Upload
        if not upload or not modified_records:
            return

        tmp_file_fd, tmp_file = tempfile.mkstemp(
            suffix='.xml',
            prefix="bibcheckfile_%s" % time.strftime("%Y-%m-%d_%H:%M:%S"),
            dir=cfg['CFG_TMPSHAREDDIR']
        )
        os.write(tmp_file_fd, records_xml)
        os.close(tmp_file_fd)
        os.chmod(tmp_file, 0644)
        if holdingpen:
            flag = "-o"
        else:
            flag = "-r"
        task = task_low_level_submission('bibupload', 'bibcheck', flag, tmp_file)
        # TODO:
        # write_message("Submitted bibupload task %s" % task)

    return _upload_amendments
