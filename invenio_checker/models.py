# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Database models for Checker module."""

import inspect
import simplejson as json
from invenio_base.wrappers import lazy_import

from sqlalchemy import types
from intbitset import intbitset  # pylint: disable=no-name-in-module
from invenio_ext.sqlalchemy import db
from invenio_search.api import Query
from invenio_records.models import RecordMetadata
from invenio_ext.sqlalchemy.utils import session_manager
from datetime import datetime, date
from bson import json_util  # included in `pymongo`, not `bson`

from .registry import plugin_files, reporters_files

from sqlalchemy_utils.types.choice import ChoiceType
from invenio_checker.clients.master import StatusMaster, RedisMaster

from operator import itemgetter
from itertools import groupby
from sqlalchemy.orm import backref
from sqlalchemy.ext import mutable
from sqlalchemy import event


def _default_date(obj):
    """`date` serializer for json."""
    try:
        return json_util.default(obj)
    except TypeError:
        if isinstance(obj, date):
            return {"$date_only": obj.isoformat()}
    raise TypeError("%r is not JSON serializable" % obj)


def _object_hook_date(dct):
    """`date` deserializer for json."""
    if "$date_only" in dct:
        isoformatted = dct["$date_only"]
        return date(*(int(i) for i in isoformatted.split('-')))
    else:
        return json_util.object_hook(dct)


class JsonEncodedDict(db.TypeDecorator):
    """Enables JSON storage by encoding and decoding on the fly."""
    impl = db.String

    def process_bind_param(self, value, dialect):
        return json.dumps(value, default=_default_date)

    def process_result_value(self, value, dialect):
        return json.loads(value, object_hook=_object_hook_date)


mutable.MutableDict.associate_with(JsonEncodedDict)


class IntBitSetType(types.TypeDecorator):

    """SQLAlchemy column type for storing compressed intbitsets"""

    impl = types.BLOB

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return intbitset(value).fastdump()

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return intbitset(value)


class CheckerRule(db.Model):

    """Represent runnable rules (also known as tasks)."""

    __tablename__ = 'checker_rule'

    name = db.Column(
        db.String(127),
        primary_key=True,
        doc="Name of the rule. Must be unique and user-friendly.",
    )

    plugin = db.Column(
        db.String(127),
        nullable=False,
        doc="Check to use. Must be importable string. Does not need to exist"
        " at task insertion time.",
    )

    arguments = db.Column(
        JsonEncodedDict(1023),
        default={},
        doc="Arguments to pass to the check.",
    )

    # XXX: Currently unsupported by search. Disabled elsewhere in the code.
    consider_deleted_records = db.Column(
        db.Boolean,
        nullable=True,
        default=False,
        doc="Whether to consider deleted records while filtering.",
    )

    filter_pattern = db.Column(
        db.String(255),
        nullable=True,
        doc="String pattern to search with to resolve records to check.",
    )

    filter_records = db.Column(
        IntBitSetType(1023),
        nullable=True,
        doc="Record IDs to run this task on.",
    )

    records = db.relationship(
        'CheckerRecord',
        backref='rule',
        cascade='all, delete-orphan',
        doc="Records which this rule has worked with in the past.",
    )

    reporters = db.relationship(
        'CheckerReporter',
        backref='rule',
        cascade='all, delete-orphan',
        doc="Reporters to be called while this task executes.",
    )

    executions = db.relationship(
        'CheckerRuleExecution',
        backref='rule',
        cascade='all, delete-orphan',
        doc="Past executions of this task. User should be free to clear them.",
    )

    last_scheduled_run = db.Column(
        db.DateTime(),
        nullable=True,
        doc="Last time this task was ran by the scheduler.",
    )

    schedule = db.Column(
        db.String(255),
        nullable=True,
        doc="Cron-style string that defines the schedule for this task.",
    )

    schedule_enabled = db.Column(
        db.Boolean,
        default=True,
        nullable=False,
        doc="Whether `schedule` is enabled.",
    )

    # TODO: You may use this column as a filter for tasks you don't want to see
    # by default in interfaces
    temporary = db.Column(
        db.Boolean,
        default=False,
        doc="Flag for tasks which will not be reused.",
    )

    force_run_on_unmodified_records = db.Column(
        db.Boolean,
        default=False,
        doc="Force a record-centric task to run on records it has checked"
        " before, even if they have already been checked in their current"
        " version.",
    )

    confirm_hash_on_commit = db.Column(
        db.Boolean,
        default=False,
        doc="Only commit recids whose hash has not changed between first"
        " requested modification and commit time.",
    )

    allow_chunking = db.Column(  # XXX unclear name (maybe "run_in_parallel")
        db.Boolean,
        default=True,
        doc="If the check is record-centric, allow checks to run in parallel.",
    )

    last_modification_date = db.Column(
        db.DateTime(),
        nullable=False,
        server_default='1900-01-01 00:00:00',
        doc="Last date on which this task was modified.",
    )

    owner_id = db.Column(
        db.Integer(15, unsigned=True),
        db.ForeignKey('user.id'),
        nullable=False,
        default=1,
    )
    owner = db.relationship(
        'User',
        doc="User that created this task. Used for scheduled tasks.",
    )

    @db.hybrid_property
    def filepath(self):
        """Resolve a the filepath of this rule's plugin/check file."""
        try:
            path = inspect.getfile(plugin_files[self.plugin])
        except KeyError:
            return None
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    @db.hybrid_property
    def modified_requested_recids(self):
        """Record IDs of records that match the filters of this task.

        This property takes (0) `requested_ids`, (1) `filter_pattern` and if
        `force_run_on_unmodified_records` is enabled (2)
        `CheckerRecord.last_run_version_id` into consideration to figure out
        which recids a record-centric task should run on.

        :rtype: intbitset
        """
        # Get all records that are already associated to this rule
        # If this is returning an empty set, you forgot to run bibindex
        try:
            associated_records = intbitset(zip(
                *db.session
                .query(CheckerRecord.rec_id)
                .filter(
                    CheckerRecord.rule_name == self.name
                ).all()
            )[0])
        except IndexError:
            associated_records = intbitset()

        # Store requested records that were until now unknown to this rule
        requested_ids = self.requested_recids
        for requested_id in requested_ids - associated_records:
            new_record = CheckerRecord(rec_id=requested_id,
                                       rule_name=self.name)
            db.session.add(new_record)
        db.session.commit()

        # Figure out which records have been edited since the last time we ran
        # this rule
        try:
            recids = zip(
                *db.session
                .query(CheckerRecord.rec_id)
                .outerjoin(RecordMetadata)
                .filter(
                    CheckerRecord.rec_id.in_(requested_ids),
                    CheckerRecord.rule_name == self.name,
                    db.or_(
                        self.force_run_on_unmodified_records,
                        db.or_(
                            CheckerRecord.last_run_version_id == 1,
                            CheckerRecord.last_run_version_id < RecordMetadata.version_id,
                        ),
                    )
                )
            )[0]
        except IndexError:
            recids = set()
        return intbitset(recids)

    @session_manager
    def mark_recids_as_checked(self, recids):
        """Mark the given recids as checked by this task at their current `version_id`."""
        db.session.query(CheckerRecord).\
            filter(
                CheckerRecord.rec_id == RecordMetadata.id,
                CheckerRecord.rule_name == self.name,
                CheckerRecord.rec_id.in_(recids),
            ).\
            update({"last_run_version_id": RecordMetadata.version_id},
                   synchronize_session=False)

    @db.hybrid_property
    def requested_recids(self):
        """Search given `self.filter_pattern` and `self.filter_records`.

        :rtype: intbitset"""
        # TODO: Use self.option_consider_deleted_records when it's available
        pattern = self.filter_pattern or ''
        recids = Query(pattern).search().recids

        if self.filter_records is not None:
            recids &= self.filter_records

        return recids

    def __str__(self):
        name_len = len(self.name)
        trails = 61 - name_len
        return '\n'.join((
            '=== Checker Task: {} {}'.format(self.name, trails * '='),
            '* Name: {}'.format(self.name),
            '* Plugin: {}'.format(self.plugin),
            '* Arguments: {}'.format(self.arguments),
            '* Consider deleted records: {}'.format(
                self.consider_deleted_records),
            '* Filter Pattern: {}'.format(self.filter_pattern),
            '* Filter Records: {}'.format(ranges_str(self.filter_records)),
            '* Last scheduled run: {}'.format(self.last_scheduled_run),
            '* Schedule: {} [{}]'.format(self.schedule, 'enabled' if
                                         self.schedule_enabled else 'disabled'),
            '* Temporary: {}'.format(self.temporary),
            '* Force-run on unmodified records: {}'
            .format(self.force_run_on_unmodified_records),
            '{}'.format(80 * '='),
        ))

    @staticmethod
    def update_time(mapper, connection, instance):
        """Update the `last_modification_date` to the current time."""
        instance.last_modification_date = datetime.now()

event.listen(CheckerRule, 'before_insert', CheckerRule.update_time)
event.listen(CheckerRule, 'before_update', CheckerRule.update_time)


class CheckerRuleExecution(db.Model):

    __tablename__ = 'checker_rule_execution'

    uuid = db.Column(
        db.String(36),
        primary_key=True,
        doc="UUID of the execution. Same with that of RedisMaster and logfile.",
    )

    owner_id = db.Column(
        db.Integer(15, unsigned=True),
        db.ForeignKey('user.id'),
        nullable=False,
        default=1,
    )
    owner = db.relationship(
        'User',
        doc="User who owns this execution. May be used by reporters.",
    )

    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name'),
        nullable=False,
        index=True,
        doc="Name of the associated task.",
    )

    _status = db.Column(
        ChoiceType(StatusMaster, impl=db.Integer()),
        default=StatusMaster.unknown,
    )

    status_update_date = db.Column(
        db.DateTime(),
        nullable=False,
        server_default='1900-01-01 00:00:00',
        doc="Last date the status was updated.",
    )

    start_date = db.Column(
        db.DateTime(),
        nullable=False,
        server_default='1900-01-01 00:00:00',
        doc="Date at which this task was started.",
    )

    dry_run = db.Column(
        db.Boolean,
        default=False,
        doc="Whether this execution is a dry run. Note the `should_*` properties."
    )

    @db.hybrid_property
    def should_commit(self):
        """Whether this execution should commit record modifications."""
        return not self.dry_run

    @db.hybrid_property
    def should_report_logs(self):
        """Whether this execution should report logs to the reporters."""
        return not self.dry_run

    @db.hybrid_property
    def should_report_exceptions(self):
        """Whether this execution should report exceptions to the reporters."""
        return not self.dry_run

    @db.hybrid_property
    def master(self):
        """The master object of this execution.

        :rtype: `invenio_checker.clients.master.RedisMaster`
        """
        return RedisMaster(self.uuid)

    @db.hybrid_property
    def status(self):
        """The status of the execution.

        :rtype: `StatusMaster`
        """
        return self._status

    @status.setter
    @session_manager
    def status(self, new_status):
        """Status setter.

        :type new_status: `StatusMaster`
        """
        self._status = new_status
        self.status_update_date = datetime.now()

    def read_logs(self):
        """Stream user-friendly structured logs of this execution.

        First attempt to stream using `eliot-tree` which provides a text
        tree-like structure of the execution given its logs.

        If `eliot-tree` fails (which happens when there is an eliot Task
        serialization bug in our code, a warning is yielded, followed by the
        output of `eliot-prettyprint`.

        Therefore, the output of this function is not guaranteed to be
        machine-readable.

        ..note::
            This function may be called mid-run.
        """
        from glob import glob
        import subprocess
        from .config import get_eliot_log_file

        filenames = glob(get_eliot_log_file(master_id=self.uuid).name + "*")
        eliottree_subp = subprocess.Popen(['eliot-tree', '--field-limit', '0'],
                                          stdout=subprocess.PIPE,
                                          stdin=subprocess.PIPE)
        eliottree_failed = False
        with eliottree_subp.stdin:
            try:
                for filename in filenames:
                    with open(filename, 'r') as file_:
                            eliottree_subp.stdin.write(file_.read())
            except (IOError, MemoryError):
                eliottree_failed = True

        with eliottree_subp.stdout:
            for line in eliottree_subp.stdout:
                yield line

        if eliottree_failed or (eliottree_subp.wait() != 0):
            # eliot-tree can fail on unfinished logging. We still want output
            # for debugging, so we use the less structured eliot-prettyprint
            from eliot.prettyprint import pretty_format
            from eliot._bytesjson import loads
            yield '\n`eliot-tree` failed to format output. ' \
                'Retrying with eliot-prettyprint:\n'
            for filename in filenames:
                yield "{}:\n".format(filename)
                with open(filename, 'r') as file_:
                    for line in file_:
                        yield pretty_format(loads(line))


class CheckerRecord(db.Model):

    """Connect checks with their executions on records."""

    __tablename__ = 'checker_record'

    rec_id = db.Column(
        db.MediumInteger(8, unsigned=True),
        db.ForeignKey(RecordMetadata.id),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    )
    record = db.relationship(
        RecordMetadata,
        backref=backref("checker_record", cascade="all, delete-orphan"),
        doc="The record associated with a task.",
    )

    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name'),
        nullable=False,
        index=True,
        primary_key=True,
        doc="Name of the task in this associaton."
    )

    last_run_version_id = db.Column(
        db.Integer,
        nullable=False,
        doc="Last checked version ID of associated record.",
    )


class CheckerReporter(db.Model):

    """Represent reporters associated with a task.

    ..note::
        These entries are currently not meant to be associated with multiple
        tasks, because it is assumed that they may be deleted without affecting
        more than one tasks.
    """

    __tablename__ = 'checker_reporter'

    plugin = db.Column(
        db.String(127),
        primary_key=True,
        doc="Check associated with this reporter."""
    )

    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name', onupdate="CASCADE", ondelete="CASCADE"),
        index=True,
        nullable=False,
        primary_key=True,
        doc="Task associated with this reporter."""
    )

    arguments = db.Column(
        JsonEncodedDict(1023),
        default={},
        doc="Arguments to be passed to this reporter.",
    )

    @db.hybrid_property
    def module(self):
        """Python module of the associated check."""
        return reporters_files[self.plugin]


def _get_ranges(items):
    """Convert a set of integers to sorted lists of sorted grouped ranges.

    example: {1,2,3,10,5} --> [[1, 2, 3], [5], [10]]
    """
    if items is None:
        items = set()
    for _, g in groupby(enumerate(sorted(set(items))), lambda (i, x): i - x):
        yield map(itemgetter(1), g)  # pylint: disable=bad-builtin

def ranges_str(items):
    """Convert a set of integers to ordered textual ranges.

    example: {1,2,3,10,5} --> '1-3,5,10'
    """
    try:
        if items.is_infinite():
            return ''
    except AttributeError:
        # Not intbitset
        pass
    output = ""
    for cont_set in _get_ranges(items):
        if len(cont_set) == 1:
            output = output + "," + str(cont_set[0])
        else:
            output = output + "," + str(cont_set[0]) + "-" + str(cont_set[-1])
    output = output.lstrip(",")
    return output

def get_reporter_db(plugin, rule_name):
    """Interface for fetching a `CheckerReporter`.

    Exists so as to not forget to use both keys when querying.

    plugin example: invenio_checker.foo.bar.baz
    rule_name example

    :rtype CheckerReporter:

    :raises: sqlalchemy.orm.exc.NoResultFound
    """
    return CheckerReporter.query.filter_by(
        rule_name=rule_name,
        plugin=plugin).one()

__all__ = (
    'CheckerRule',
    'CheckerRecord',
    'CheckerReporter',
    'CheckerRuleExecution'
)
