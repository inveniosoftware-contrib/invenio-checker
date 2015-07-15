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

from invenio.ext.sqlalchemy import db
from invenio_records.models import Record as Bibrec
from sqlalchemy import orm

from .errors import PluginMissing
from .registry import plugin_files


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
    filter_field = db.Column(db.String(255), nullable=True)
    filter_limit = db.Column(db.Integer(unsigned=True), nullable=True)

    records = db.relationship('CheckerRecord', backref='checker_rule',
                              cascade='all, delete-orphan')

    @db.hybrid_property
    def filter(self):
        return {
            'pattern': self.filter_pattern,
            'field': self.filter_field,
            'limit': self.filter_limit,
        }

    @db.hybrid_property
    def option(self):
        return {
            'holdingpen': self.option_holdingpen,
            'consider_deleted_records': self.option_consider_deleted_records,
        }

    @orm.reconstructor
    def init_on_load(self):
        from .rules import Query
        self.query_ex = Query(self.filter, self.option)
        if self.pluginspec not in plugin_files:
            raise PluginMissing(self.pluginspec, self.name)

    @classmethod
    def from_name(cls, name):
        return cls.query.filter(CheckerRule.name==name).one()

    @property
    def pluginspec(self):
        """Resolve checkspec of the rule's check."""
        return '{module}.checkerext.checks.{file}'\
            .format(module=self.plugin_module, file=self.plugin_file)

    @property
    def filepath(self):
        """Resolve a the filepath of this rule's plugin."""
        path = inspect.getfile(plugin_files[self.pluginspec])
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    # @cached_property
    def modified_records(self, user_ids):
        # Get all records that are already associated to this rule
        # If this is returning an empty set, you forgot to run bibindex
        try:
            associated_records = zip(
                *db.session
                .query(CheckerRecord.id_bibrec)
                .filter(
                    CheckerRecord.name_checker_rule==self.name
                ).all()
            )[0]
        except IndexError:
            associated_records = []

        # Store requested records that were until now unknown to this rule
        requested_ids = self.query.requested_ids(user_ids)
        for requested_id in requested_ids:
            if requested_id not in associated_records:
                new_record = CheckerRecord(id_bibrec=requested_id,
                                           name_checker_rule=self.name)
                db.session.add(new_record)
        db.session.commit()

        # Figure out which records have been edited since the last time we ran
        # this rule
        try:
            return zip(
                *db.session
                .query(CheckerRecord.id_bibrec)
                .outerjoin(Bibrec)
                .filter(
                    db.and_(
                        CheckerRecord.id_bibrec.in_(requested_ids),
                        CheckerRecord.name_checker_rule == self.name,
                        db.or_(
                            CheckerRecord.last_run < Bibrec.modification_date,
                            db.and_(
                                CheckerRecord.last_run > Bibrec.modification_date,
                                CheckerRecord.expecting_modification == True
                            )
                        )
                    )
                )
            )[0]
        except IndexError:
            return []



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

    expecting_modification = db.Column(db.Boolean, nullable=False, default=False)

    last_run = db.Column(db.DateTime, nullable=False,
                         server_default='1900-01-01 00:00:00', index=True)

__all__ = ('CheckerRule', 'CheckerRecord', )
