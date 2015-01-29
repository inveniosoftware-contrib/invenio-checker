# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2013, 2014, 2015 CERN.
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

"""Wrap records for workflows of checker module."""

from invenio.modules.records.api import Record

class Issue(object):
    """Holds information about a single record issue."""

    def __init__(self, nature, rule, msg):
        self._nature = None
        self.nature = nature
        self.rule = rule
        self.msg = msg

    @property
    def nature(self):
        return self._nature

    @nature.setter
    def nature(self, value):
        assert value in ('error', 'amendment', 'warning')
        self._nature = value


class AmendableRecord(Record):
    """Class that wraps a record (recstruct) to pass to a plugin."""

    def __init__(self, json, rule_name, **kwargs):
        super(AmendableRecord, self).__init__(json, **kwargs)
        assert not hasattr(self, 'check'), ("Record already has a `check` "
                                            "attribute. Programming error.")
        self.check = Check(self['recid'], rule_name)


class Check(object):
    def __init__(self, record_id, rule_name):
        self.issues = []
        self.valid = True
        self.rule_name = rule_name
        self.record_id = record_id

    @property
    def _errors(self):
        return [i for i in self.issues if i.nature == 'error']

    @property
    def _amendments(self):
        return [i for i in self.issues if i.nature == 'amendment']

    @property
    def _warnings(self):
        return [i for i in self.issues if i.nature == 'warning']

    def set_invalid(self, reason):
        """Mark the record as invalid."""
        # url = "{site}/{record}/{record_id}".format(site=CFG_SITE_URL,
        #                                            record=CFG_SITE_RECORD,
        #                                            record_id=self.record_id)
        # write_message("Record {url} marked as invalid by rule {name}: {reason}".
        #               format(url=url, name=self.rule_name["name"], reason=reason))
        self.issues.append(Issue('error', self.rule_name['name'], reason))
        self.valid = False

    def warn(self, msg):
        """Add a warning to the record."""
        self.issues.append(Issue('warning', self.rule_name['name'], msg))
        # write_message("[WARN] record %s by rule %s: %s" %
        #         (self.record_id, self.rule_name["name"], msg))
