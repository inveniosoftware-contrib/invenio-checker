# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2014 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Database models for Checker module."""

from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import composite

from .sqlalchemyext.json_encoded_dict import MutableDict, JSONEncodedDict
from invenio.ext.sqlalchemy import db


class CompositeItems(MutableComposite):

    def __composite_items__(self):
        """Return list of supported names.

        This is neccessary for serialization since we have no other way of
        knowing which properties to read.
        """
        raise NotImplementedError

    def __composite_orig_keys__(self):
        raise NotImplementedError

    def __composite_mapper__(self):
        raise NotImplementedError


class CompositeChecker(CompositeItems):

    def __init__(self):
        """Ensure that there are no mistakes in the mapper."""
        for key in self.__composite_mapper__().keys():
            assert hasattr(self, key)

    def __repr__(self):
        repr_values = ', '.join(self.__composite_items__())
        return "{cls}: ({values})".format(cls=type(self).__name__,
                                          values=str(repr_values))

    def __setattr__(self, key, value):
        "Intercept set events."
        object.__setattr__(self, key, value)
        self.changed()

    def __composite_values__(self):
        for key, val in self.__composite_items__():
            yield val

    def __composite_keys__(self):
        for key, val in self.__composite_items__():
            yield key

    def __composite_items__(self):
        for key, val in self.__composite_mapper__().items():
            yield key, getattr(self, key)

    def __composite_orig_keys__(self):
        for orig_key in self.__composite_mapper__().values():
            yield orig_key


class Plugin(CompositeChecker):
    def __init__(self, module, file):
        self.module = module
        self.file = file
        super(Plugin, self).__init__()

    def __composite_mapper__(self):
        return {'module': 'plugin_module',
                'file': 'plugin_file'}

    def __eq__(self, other):
        return isinstance(other, Plugin) and \
            other.module == self.file and \
            other.module == self.module

    def __ne__(self, other):
        return not self.__eq__(other)


class Option(CompositeChecker):
    def __init__(self, holdingpen, consider_deleted_records):
        self.holdingpen = holdingpen
        self.consider_deleted_records = consider_deleted_records
        super(Option, self).__init__()

    def __composite_mapper__(self):
        return {'holdingpen': 'option_holdingpen',
                'consider_deleted_records': 'option_consider_deleted_records'}


class Filter(CompositeChecker):
    def __init__(self, pattern, field, limit):
        self.pattern = pattern
        self.field = field
        self.limit = limit
        super(Filter, self).__init__()

    def __composite_mapper__(self):
        return {'pattern': 'filter_pattern',
                'field': 'filter_field',
                'limit': 'filter_limit'}


class CheckerRule(db.Model):

    """Represent runnable rules."""

    __tablename__ = 'checker_rule'

    name = db.Column(db.String(50), primary_key=True)

    plugin_module = db.Column(db.String(50), nullable=False)
    plugin_file = db.Column(db.String(50), nullable=False)
    plugin = composite(Plugin, plugin_module, plugin_file)

    arguments = db.Column(MutableDict.as_mutable(JSONEncodedDict), default={})

    option_holdingpen = db.Column(db.Boolean, nullable=False, default=True)
    option_consider_deleted_records = db.Column(db.Boolean, nullable=True,
                                                 default=False)
    option = composite(Option, option_holdingpen,
                       option_consider_deleted_records)

    filter_pattern = db.Column(db.String(255), nullable=True)
    filter_field = db.Column(db.String(255), nullable=True)
    filter_limit = db.Column(db.Integer(unsigned=True), nullable=True)
    filter = composite(Filter, filter_pattern, filter_field, filter_limit)

    records = db.relationship('CheckerRecord', backref='checker_rule',
                              cascade='all, delete-orphan')


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
