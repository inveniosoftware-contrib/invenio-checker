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

"""Rule handlers for checker module."""

import json
from collections import defaultdict, MutableSequence
from importlib import import_module
from intbitset import intbitset
from werkzeug.utils import cached_property

from .common import ALL
from .errors import DuplicateRuleError
from invenio.ext.sqlalchemy import db
from invenio.legacy.search_engine import search_pattern
from invenio.modules.checker.models import CheckerRule, CheckerRecord
from invenio.modules.records.models import Record as Bibrec


class Query(object):

    def __init__(self, filter, option):
        self._filter = filter
        self._option = option
        self.known_requested_ids = {}

    def requested_ids(self, user_ids):
        """Get a user-filtered list of IDs requested by this rule.

        :param user_ids: intbitset of requested IDs or ALL
        :type  user_ids: list or str

        :returns: seq of IDs found in the database
        :rtype:   intbitset

        Trusts that we do not modify self._filter and self._option
        """
        # Have we calculated the hitset for these `user_ids` before?
        try:
            return self.known_requested_ids[user_ids]
        except KeyError:
            pass

        def ids_eq_all():
            # HACK around https://github.com/inveniosoftware/intbitset/issues/18
            return isinstance(user_ids, type(ALL)) and user_ids == ALL

        ret_ids = self._run_query()
        if ids_eq_all():  # TODO: if user_ids == ALL:
            user_ids = intbitset(trailing_bits=1)
        self.known_requested_ids[user_ids] = ret_ids & user_ids
        return self.known_requested_ids[user_ids]

    def _query_filters(self, force_finiteness=False):
        """Compatibalize config filters with `search_pattern` args.

        :returns: sets for expansion in `search_pattern`
        :rtype:   dict
        """
        cfg_mapper = {
            'wl': 'limit',
            'p': 'pattern',
            'f': 'field',
        }
        for query_arg, filter_name in cfg_mapper.items():
            try:
                # Example: ('p', 'Higgs')
                yield (query_arg, self._filter[filter_name])
            except KeyError:
                pass
        # HACK: Trick `search_pattern` into not returning inf.
        try:
            self._filter['p']
        except KeyError:
            force_finiteness = True
        if force_finiteness:
            yield ('p', '* AND *')

    def _query_options(self):
        """Convert the options section of a rule into query arguments."""
        # HACK: Force `search_pattern` to return deleted records.
        try:
            if self._option['consider_deleted_records']:
                return {'ap': -9, 'req': None}
            else:
                return {}
        except KeyError:
            return {}
        # TODO: ['option']['holdingpen']

    def _run_query(self):
        """Query database for records based on rule configuration."""
        query_kwargs = {}
        query_kwargs.update(self._query_filters())
        query_kwargs.update(self._query_options())
        result = search_pattern(**query_kwargs)
        if result.is_infinite():
            query_kwargs.update(self._query_filters(force_finiteness=True))
            result = search_pattern(**query_kwargs)
        assert not result.is_infinite(), '\n'.join((
            '',
            '`search_pattern` now works,'
            'alas; you must now amend',
            'my delicate workarounds.',
        ))
        return result


class Rule(dict):
    """Interface for a single rule."""

    def __init__(self, *args, **kwargs):
        """Initialize a single rule.

        It is the programmer's responsibility to decide when to do validation,
        because it is strict about extraneous values which may be useful to have
        in the dict.
        """
        super(Rule, self).__init__(*args, **kwargs)
        if 'filter' not in self:
            self['filter'] = {}
        self.query = Query(self['filter'], self['option'])

    @classmethod
    def from_name(cls, name):
        try:
            rule = CheckerRule.query.filter(CheckerRule.name==name).all()[0]
        except IndexError as e:
            e.args += ("Requested rule {} not found in the database."
                       .format(name),)
            raise e
        else:
            return cls((rule.todict(composites=True)))

    @property
    def pluginspec(self):
        """Resolve checkspec of the rule's check."""
        return 'invenio.modules.{module}.checkerext.plugins.{file}'\
            .format(module=self['plugin']['module'], file=self['plugin']['file'])

    # @cached_property
    def modified_records(self, user_ids):
        # Get all records that are already associated to this rule
        try:
            associated_records = zip(
                *db.session
                .query(CheckerRecord.id_bibrec)
                .filter(
                    CheckerRecord.name_checker_rule==self['name']
                ).all()
            )[0]
        except IndexError:
            associated_records = []

        # Store requested records that were until now unknown to this rule
        requested_ids = self.query.requested_ids(user_ids)
        for requested_id in requested_ids:
            if requested_id not in associated_records:
                new_record = CheckerRecord(id_bibrec=requested_id,
                                           name_checker_rule=self['name'])
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
                        CheckerRecord.name_checker_rule == self['name'],
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

    @classmethod
    def from_json(cls, rule_json):
        """Create a Rule from json.

        :param rule_json: a json representation of a Rule's dictionary
        :type  rule_json: str
        :returns: a single rule
        :rtype:   Rule instance

        :raises: ValueError
        """
        return cls(json.loads(rule_json))

    def to_json(self):
        """Save a JSON-friendly representation of the rule.

        :returns: a json representation of a Rule's dictionary
        :rtype:   str
        """
        return json.dumps(self)

    @cached_property
    def is_batch(self):
        """Check if a rule uses a batch plugin.

        Lazy, trusts that we do not modify self['plugin'].
        :returns: whether a rule uses a batch plugin or not
        :rtype:   bool

        :raises: ImportError
        """
        plugin_module = import_module(self.pluginspec)
        return hasattr(plugin_module, 'pre_check')


class Rules(MutableSequence):
    """Maintain a set of rules."""

    def __init__(self, *args):
        self.list = list()
        self.extend(list(args))

    def check(self, document):
        """Prevent rules with the same name from being added to the list.

        :raises: DuplicateRuleError
        """
        rule_name = document['name']
        if rule_name in (rule['name'] for rule in self):
            raise DuplicateRuleError(rule_name)

    def __len__(self):
        return len(self.list)

    def __getitem__(self, i):
        return self.list[i]

    def __delitem__(self, i):
        del self.list[i]

    def __setitem__(self, i, v):
        self.check(v)
        self.list[i] = v

    def insert(self, i, v):
        self.check(v)
        self.list.insert(i, v)

    def __str__(self):
        return str(self.list)

    @classmethod
    def from_input(cls, user_choice):
        """Return the rules that should run from user input.

        :param user_choice: comma-separated list of rule specifiers
            example: 'checker.my_rule,checker.other_rule'
        :type  user_choice: str

        :returns: Rules
        :rtype:   seq
        """
        user_rules = set(user_choice.split(','))
        rules = cls()
        if ALL in user_rules:
            rules.load_rule(ALL)
            while ALL in rules:
                rules.pop(ALL)
        for rule_name in user_rules:
            rules.load_rule(rule_name)
        return rules

    @classmethod
    def from_jsons(cls, rule_jsons):
        """Recover Rules from a JSON string.

        This is used by workflow workers.
        """
        rules = cls()
        for rule_json in rule_jsons:
            rule = Rule.from_json(rule_json)
            rules.append(rule)
        return rules

    def iterbatch(self):
        """Iterate over batch rules."""
        for rule in self:
            if rule.is_batch:
                yield rule

    def itersimple(self):
        """Iterate over simple rules."""
        for rule in self:
            if not rule.is_batch:
                yield rule

    def by_json_ruleset(self, user_ids):
        """Bundle rules of the specified user IDs into by-rule-set sets.

        :param user_ids: IDs to be bundled
        :type  user_ids: list

        :returns: seq(json(rule)):relevant_ids
        :rtype:   dict
        """
        # Bundle into ID:json(rule)
        id_bundles = defaultdict(list)
        for rule in self:
            rule.user_ids = user_ids
            for id_ in rule.modified_records(user_ids):
                rule_json = json.dumps(rule)
                id_bundles[id_].append(rule_json)
        # Bundle into seq(json(rule)):ids
        bundles = defaultdict(list)
        for id_, rule_json in id_bundles.iteritems():
            bundles[tuple(rule_json)].append(id_)
        return bundles

    def load_rule(self, rule_name):
        """Load rules from the database.

        :param rule_name: rule name or ALL

        :raises: IndexError
        """
        if rule_name == ALL:
            for db_rule in CheckerRule.query.all():
                self.append(Rule(db_rule.todict(composites=True)))
        else:
            self.append(Rule.from_name(rule_name))
