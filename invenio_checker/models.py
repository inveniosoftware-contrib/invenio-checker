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

from sqlalchemy import types
from intbitset import intbitset  # pylint: disable=no-name-in-module
from invenio.ext.sqlalchemy import db
from invenio_search.api import Query
from invenio_records.models import Record as Bibrec
from sqlalchemy import orm
from invenio.ext.sqlalchemy.utils import session_manager
from datetime import datetime, date
from bson import json_util  # included in `pymongo`, not `bson`
from invenio_accounts.models import User

from .common import ALL
from .errors import PluginMissing
from .registry import plugin_files, reporters_files

from sqlalchemy_utils.types.choice import ChoiceType
from .master import StatusMaster

from sqlalchemy.ext import mutable


def default_date(obj):
    try:
        return json_util.default(obj)
    except TypeError:
        if isinstance(obj, date):
            return {"$date_only": obj.isoformat()}
    raise TypeError("%r is not JSON serializable" % obj)


def object_hook_date(dct):
    if "$date_only" in dct:
        isoformatted = dct["$date_only"]
        return date(*(int(i) for i in isoformatted.split('-')))
    else:
        return json_util.object_hook(dct)


class JsonEncodedDict(db.TypeDecorator):
    """Enables JSON storage by encoding and decoding on the fly."""
    impl = db.String

    def process_bind_param(self, value, dialect):
        return json.dumps(value, default=default_date)

    def process_result_value(self, value, dialect):
        return json.loads(value, object_hook=object_hook_date)


mutable.MutableDict.associate_with(JsonEncodedDict)


class IntBitSetType(types.TypeDecorator):

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

    """Represent runnable rules."""

    __tablename__ = 'checker_rule'

    name = db.Column(db.String(127), primary_key=True)

    plugin = db.Column(db.String(127), nullable=False)

    arguments = db.Column(JsonEncodedDict(1023), default={})

    consider_deleted_records = db.Column(db.Boolean, nullable=True,
                                         default=False)

    filter_pattern = db.Column(db.String(255), nullable=True)

    filter_records = db.Column(IntBitSetType(1023), nullable=True)

    records = db.relationship('CheckerRecord', backref='rule',
                              cascade='all, delete-orphan')

    reporters = db.relationship('CheckerReporter', backref='rule',
                                cascade='all, delete-orphan')

    executions = db.relationship('CheckerRuleExecution', backref='rule',
                                 cascade='all, delete-orphan')

    last_run = db.Column(db.DateTime(), nullable=True)

    schedule = db.Column(db.String(255), nullable=True)

    schedule_enabled = db.Column(db.Boolean, default=True, nullable=False)

    temporary = db.Column(db.Boolean, default=False)  # TODO: use

    force_run_on_unmodified_records = db.Column(db.Boolean, default=False)

    # TODO
    # commit = db.Column(db.Boolean, default=True)

    @db.hybrid_property
    def filepath(self):
        """Resolve a the filepath of this rule's plugin."""
        path = inspect.getfile(plugin_files[self.plugin])
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    @db.hybrid_property
    def modified_requested_recids(self):
        """
        :rtype: intbitset
        """
        # Get all records that are already associated to this rule
        # If this is returning an empty set, you forgot to run bibindex
        try:
            associated_records = intbitset(zip(
                *db.session
                .query(CheckerRecord.bibrec_id)
                .filter(
                    CheckerRecord.rule_name == self.name
                ).all()
            )[0])
        except IndexError:
            associated_records = intbitset()

        # Store requested records that were until now unknown to this rule
        requested_ids = self.requested_recids
        for requested_id in requested_ids - associated_records:
            new_record = CheckerRecord(bibrec_id=requested_id,
                                       rule_name=self.name)
            db.session.add(new_record)
        db.session.commit()

        # Figure out which records have been edited since the last time we ran
        # this rule
        try:
            recids = zip(
                *db.session
                .query(CheckerRecord.bibrec_id)
                .outerjoin(Bibrec)
                .filter(
                    CheckerRecord.bibrec_id.in_(requested_ids),
                    CheckerRecord.rule_name == self.name,
                    db.or_(
                        self.force_run_on_unmodified_records,
                        db.or_(
                            CheckerRecord.last_run == None,
                            CheckerRecord.last_run < Bibrec.modification_date,
                        ),
                    )
                )
            )[0]
        except IndexError:
            recids = set()
        return intbitset(recids)

    @session_manager
    def mark_recids_as_checked(self, recids):
        now = datetime.now()
        db.session.query(CheckerRecord).filter(
            db.and_(
                CheckerRecord.bibrec_id == recids,
                CheckerRecord.rule_name == self.name,
            )
        ).update(
            {
                "last_run": now,
            },
            synchronize_session=False
        )

    @db.hybrid_property
    def requested_recids(self):
        """Search using config only."""
        # TODO: Use self.option_consider_deleted_records
        pattern = self.filter_pattern or ''
        recids = Query(pattern).search().recids

        if self.filter_records is not None:
            recids &= self.filter_records

        return recids

    @classmethod
    def from_ids(cls, rule_names):
        """Get a set of rules from their names.

        :param rule_names: list of rule names
        """
        ret = set(cls.query.filter(cls.name.in_(rule_names)).all())
        if len(rule_names) != len(ret):
            raise Exception('Not all requested rules were found in the database!')
        return ret

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
            '* Last run {}'.format(self.last_run),
            '* Schedule {} [{}]'.format(self.schedule, 'enabled' if
                                        self.schedule_enabled else 'disabled'),
            '* Temporary: {}'.format(self.temporary),
            '* Force-run on unmodified records {}'
            .format(self.force_run_on_unmodified_records),
            '{}'.format(80 * '='),
        ))


class CheckerRuleExecution(db.Model):

    __tablename__ = 'checker_rule_execution'

    uuid = db.Column(
        db.String(36),
        primary_key=True,
    )

    owner_id = db.Column(
        db.Integer(15, unsigned=True),
        db.ForeignKey('user.id'),
        nullable=False,
        server_default='0'
    )
    owner = db.relationship(
        'User'
    )

    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name'),
        nullable=False,
        index=True,
    )

    _status = db.Column(
        ChoiceType(StatusMaster, impl=db.Integer()),
        default=StatusMaster.unknown,
    )

    status_update_date = db.Column(
        db.DateTime(),
        nullable=False,
        server_default='1900-01-01 00:00:00',
    )

    start_date = db.Column(
        db.DateTime(),
        nullable=False,
        server_default='1900-01-01 00:00:00',
    )

    @db.hybrid_property
    def status(self):
        return self._status

    @status.setter
    @session_manager
    def status(self, new_status):
        self._status = new_status
        self.status_update_date = datetime.now()

    def read_logs(self):
        import os
        from glob import glob
        import subprocess
        from .config import get_eliot_log_path

        eliot_log_path = get_eliot_log_path()

        filenames = glob(os.path.join(eliot_log_path, self.uuid + "*"))

        eliottree_subp = subprocess.Popen(['eliot-tree', '--field-limit', '0'],
                                          stdout=subprocess.PIPE,
                                          stdin=subprocess.PIPE)
        with eliottree_subp.stdin:
            for filename in filenames:
                with open(filename, 'r') as file_:
                    eliottree_subp.stdin.write(file_.read())
        with eliottree_subp.stdout:
            for line in eliottree_subp.stdout:
                yield line

        # If you ever decide to switch to eliot-prettyprint..
        # from eliot.prettyprint import pretty_format
        # from eliot._bytesjson import loads
        # for filename in filenames:
        #     with open(filename, 'r') as file_:
        #         for line in file_:
        #             yield pretty_format(loads(line))


class CheckerRecord(db.Model):

    """Connect checks with their executions on records."""

    __tablename__ = 'checker_record'

    bibrec_id = db.Column(
        db.MediumInteger(8, unsigned=True),
        db.ForeignKey('bibrec.id'),
        primary_key=True, nullable=False
    )

    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name'),
        nullable=False,
        index=True,
        primary_key=True,
    )

    last_run = db.Column(
        db.DateTime,
        nullable=True,
        server_default=None,
        index=True
    )


class CheckerReporter(db.Model):

    """Represent instantiated reporters for rule."""

    __tablename__ = 'checker_reporter'

    uuid = db.Column(db.Integer, primary_key=True)
    plugin = db.Column(db.String(127))
    arguments = db.Column(JsonEncodedDict(1023), default={})
    rule_name = db.Column(
        db.String(127),
        db.ForeignKey('checker_rule.name'),
        index=True,
    )

    @db.hybrid_property
    def module(self):
        return reporters_files[self.plugin]


def _get_ranges(items):
    """Convert a set of integers to sorted lists of sorted grouped ranges.

    example: {1,2,3,10,5} --> [[1, 2, 3], [5], [10]]
    """
    from operator import itemgetter
    from itertools import groupby
    for _, g in groupby(enumerate(sorted(set(items))), lambda (i, x): i - x):
        yield map(itemgetter(1), g)  # pylint: disable=bad-builtin

def ranges_str(items):
    """Convert a set of integers to ordered textual ranges.

    example: {1,2,3,10,5} --> '1-3,5,10'
    """
    output = ""
    for cont_set in _get_ranges(items):
        if len(cont_set) == 1:
            output = output + "," + str(cont_set[0])
        else:
            output = output + "," + str(cont_set[0]) + "-" + str(cont_set[-1])
    output = output.lstrip(",")
    return output

__all__ = (
    'CheckerRule',
    'CheckerRecord',
    'CheckerReporter',
    'CheckerRuleExecution'
)
