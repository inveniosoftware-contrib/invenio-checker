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

import sqlalchemy.types as types
from intbitset import intbitset  # pylint: disable=no-name-in-module
from invenio.ext.sqlalchemy import db
from invenio_search.api import Query
from invenio_records.models import Record as Bibrec
from sqlalchemy import orm
from invenio.ext.sqlalchemy.utils import session_manager
from datetime import datetime

from .common import ALL
from .errors import PluginMissing
from .registry import plugin_files


class PickleIntBitSet(types.TypeDecorator):
    '''Prefixes Unicode values with "PREFIX:" on the way in and
    strips it off on the way out.
    '''

    impl = db.PickleType

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return intbitset(value)


class CheckerRule(db.Model):

    """Represent runnable rules."""

    __tablename__ = 'checker_rule'

    name = db.Column(db.String(50), primary_key=True)

    plugin_module = db.Column(db.String(50), nullable=False)
    plugin_file = db.Column(db.String(50), nullable=False)

    arguments = db.Column(db.PickleType, default={})

    option_holdingpen = db.Column(db.Boolean, nullable=False, default=True)
    option_consider_deleted_records = db.Column(db.Boolean, nullable=True,
                                                 default=False)

    filter_pattern = db.Column(db.String(255), nullable=True)
    filter_records = db.Column(PickleIntBitSet, nullable=True)

    records = db.relationship('CheckerRecord', backref='checker_rule',
                              cascade='all, delete-orphan')


    owner_id = db.Column(db.Integer(15, unsigned=True), db.ForeignKey('user.id'))
    owner = db.relationship('User', uselist=False,
                            backref=db.backref('checker_rule', uselist=False))

    @db.hybrid_property
    def pluginspec(self):
        """Resolve checkspec of the rule's check."""
        return '{module}.checkerext.checks.{file}'\
            .format(module=self.plugin_module, file=self.plugin_file)

    @db.hybrid_property
    def filepath(self):
        """Resolve a the filepath of this rule's plugin."""
        path = inspect.getfile(plugin_files[self.pluginspec])
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    @db.hybrid_property
    def modified_requested_recids(self):
        # Get all records that are already associated to this rule
        # If this is returning an empty set, you forgot to run bibindex
        try:
            associated_records = intbitset(zip(
                *db.session
                .query(CheckerRecord.id_bibrec)
                .filter(
                    CheckerRecord.name_checker_rule==self.name
                ).all()
            )[0])
        except IndexError:
            associated_records = intbitset()

        # Store requested records that were until now unknown to this rule
        requested_ids = self.requested_recids
        for requested_id in requested_ids - associated_records:
            new_record = CheckerRecord(id_bibrec=requested_id,
                                       name_checker_rule=self.name)
            db.session.add(new_record)
        db.session.commit()

        # Figure out which records have been edited since the last time we ran
        # this rule
        try:
            recids = zip(
                *db.session
                .query(CheckerRecord.id_bibrec)
                .outerjoin(Bibrec)
                .filter(
                    db.and_(
                        CheckerRecord.id_bibrec.in_(requested_ids),
                        CheckerRecord.name_checker_rule == self.name,
                        db.or_(
                            CheckerRecord.last_run == None,
                            CheckerRecord.last_run < Bibrec.modification_date,
                        )
                    )
                )
            )[0]
        except IndexError:
            recids = {}
        return intbitset(recids)

    @session_manager
    def mark_recids_as_checked(self, recids):
        now = datetime.now()
        db.session.query(CheckerRecord).filter(
            db.and_(
                CheckerRecord.id_bibrec == recids,
                CheckerRecord.name_checker_rule == self.name,
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
    def from_input(cls, user_rule_names):
        """Return the rules that should run from user input.

        :param user_rule_names: comma-separated list of rule specifiers
            example: 'my_rule,other_rule'
        :type  user_rule_names: str

        :returns: set of rules
        """
        rule_names = set(user_rule_names.split(','))
        if ALL in rule_names:
            return set(cls.query.all())
        else:
            return cls.from_ids(rule_names)

    @classmethod
    def from_ids(cls, rule_names):
        """Get a set of rules from their names.

        :param rule_names: list of rule names
        """
        ret = set(cls.query.filter(cls.name.in_(rule_names)).all())
        if len(rule_names) != len(ret):
            raise Exception('Not all requested rules were found in the database!')
        return ret


class CheckerRecord(db.Model):

    """Connect checks with their executions on records."""

    __tablename__ = 'checker_record'

    id_bibrec = db.Column(db.MediumInteger(8, unsigned=True),
                          db.ForeignKey('bibrec.id'),
                          primary_key=True, nullable=False)

    name_checker_rule = db.Column(db.String(50),
                                  db.ForeignKey('checker_rule.name'),
                                  nullable=False, index=True,
                                  primary_key=True)

    last_run = db.Column(db.DateTime, nullable=True, server_default=None, index=True)


__all__ = ('CheckerRule', 'CheckerRecord', )
