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

from invenio.base.utils import partial_argc
from invenio.modules.checker.rules import Rules, Rule
from invenio.modules.records.api import get_record, Record
from invenio.legacy.bibsched.bibtask import task_low_level_submission

from dictdiffer import diff
import time
import os
import tempfile
from invenio.config import CFG_TMPSHAREDDIR


def _ensure_key(key, dict_):
    if key not in dict_:
        dict_[key] = {}


def _record_has_changed(record, extra_data):
    """Check whether a record is different from its state in the database.

    :param record: record
    :type  record: invenio.modules.records.api.Record

    :returns: whether the record has changed
    :rtype:   bool
    """
    recid = record['recid']
    modified_record = record.dumps()
    # No point in doing this.
    # # Try against `extra_data`
    # try:
    #     extra_data_record = extra_data['modified_records'][recid]
    #     if tuple(diff(extra_data_record record, modified_record)):
    #         return True
    # except KeyError:
    #     # We have not previously stored this record
    #     pass
    # Try against the database
    db_record = get_record(recid).dumps()
    if tuple(diff(db_record, modified_record)):
        return True
    return False


def _store_extras(obj, extra_data, records):
    """Update `extra_data`, with its new value and the new state of records."""
    for record in records:
        if _record_has_changed(record, extra_data):
            recid = int(record['recid'])
            _ensure_key('modified_records', extra_data)
            extra_data['modified_records'][recid] = record.dumps()
    obj.extra_data = extra_data


def _load_record(extra_data, recid, local_storage_only=False):
    """Load a record by recid.

    Revives a record from `extra_data`. If the spell fails, loads it from the
    database.

    :returns: record
    :rtype: invenio.modules.records.api.Record
    """
    try:
        # Get from extra data
        return Record(extra_data['modified_records'][recid])
    except KeyError as e:
        if local_storage_only:
            e.args += ('Non-loaded record {id_} requested. Programming error(?)'
                       .format(id_=recid),)
            raise e
        # Get from the database
        record = get_record(recid)
        if not record:
            raise LookupError('Record {} vanished from the databse!'
                              .format(recid))
        return record


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
    """
    # Load everything
    extra_data = obj.get_extra_data()
    recids = extra_data['recids']
    rule = Rule(obj.data)
    records = (_load_record(extra_data, recid) for recid in recids)
    plugin_module = import_module(rule.pluginspec)

    # Execute pre- function and save retval
    _ensure_key('result_pre_check', extra_data)
    extra_data['result_pre_check'][rule['name']] = \
        plugin_module.pre_check(records)
    _store_extras(obj, extra_data, records)


def run_check(obj, eng):
    """Check a single record against a rule."""
    # Load everything
    extra_data = obj.get_extra_data()
    recid = obj.data
    rule = Rule(extra_data['rule_object'])
    record = _load_record(extra_data, recid)
    plugin_module = import_module(rule.pluginspec)

    # Initialize partial
    check_record = partial(plugin_module.check_record, record)
    # Append result_pre_check if this is part of a batch plugin
    try:
        result_pre_check = extra_data['result_pre_check'][rule['name']]
        check_record = partial(check_record, result_pre_check)
    except KeyError:
        pass
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
        expected_argcount = partial_argc(check_record)
        given_argcount = plugin_module.check_record.func_code.co_argcount
        if expected_argcount != given_argcount:
            e.args += ('Wrong plugin function signature at {code}'
                       .format(code=plugin_module.check_record.func_code),)
            # FIXME: All future calls to this specific `check_record` are doomed
            # to fail. Either set some variable to stop future `run_checks`
            # from running or do this earlier.
            # FIXME: Same goes for batch, but with greater impact.
        raise
    _store_extras(obj, extra_data, [record])


def save_records():
    """Upload all modified records."""
    @wraps(save_records)
    def _upload_amendments(obj, eng, holdingpen=False):
        # TODO
        # if task_get_option("no_upload", False) or len(records) == 0:
        #     return

        extra_data = obj.get_extra_data()
        _ensure_key('modified_records', extra_data)
        records = (Record(r)
                   for r in extra_data['modified_records'].itervalues())

        records_xml = ""
        for record in records:
            records_xml += record.legacy_export_as_marc()
        if not records_xml:
            return
        records_xml = (
            '<collection xmlns="http://www.loc.gov/MARC21/slim">\n'
            '{}'
            '</collection>'
            .format(records_xml)
        )

        # TODO: Create temp of temp and then use mv to make the operation atomic
        tmp_file_fd, tmp_file = tempfile.mkstemp(
            suffix='.xml',
            prefix="bibcheckfile_%s" % time.strftime("%Y-%m-%d_%H:%M:%S"),
            dir=CFG_TMPSHAREDDIR
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
