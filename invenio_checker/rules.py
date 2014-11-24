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
import json
import yaml
from argparse import ArgumentTypeError
from collections import defaultdict, MutableSequence
from pykwalify.core import Core
from pykwalify.errors import SchemaError
from importlib import import_module

from .registry import config_files, schema_files
from .common import ALL
from invenio.legacy.search_engine import perform_request_search


class DuplicateRuleError(Exception):
    pass


class Rule(dict):

    def __init__(self, *args, **kwargs):
        """Initialize a single rule.

        It is the programmer's responsibility to decide when to do validation,
        because it is strict about extraneous values which may be useful to have
        in the dict.
        """
        self._requested_ids = None
        self._is_batch = None
        super(Rule, self).__init__(*args, **kwargs)

    @property
    def pluginspec(self):
        """Resolve checkspec of the rule's check."""
        return 'invenio.modules.{module}.checkerext.plugins.{file}'\
            .format(module=self['plugin']['module'], file=self['plugin']['file'])

    def requested_ids(self, user_ids):
        """Get a user-filtered list of IDs requested by this rule.

        :param user_ids: list of requested IDs
            * may contain ALL
        :type  user_ids: list or str

        :returns: seq of IDs found in the database
        :rtype:   intbitset

        Lazy, trusts that we do not modify self['filter'] or change IDs.
        """
        if self._requested_ids is not None:
            return self._requested_ids
        default_args = {
            'sf': 'id',
            'so': 'd',
            'of': 'intbitset'
        }
        default_args.update(self._query_dict())
        ret_ids = perform_request_search(**default_args)
        if user_ids == ALL:
            self._requested_ids = ret_ids
        else:
            self._requested_ids = ret_ids & user_ids
        return self._requested_ids

    def _query_dict(self):
        """Compatibalize config filters with `perform_request_search` args.

        :returns: sets for expansion in `perform_request_search`
        :rtype:   dict
        """
        query_translator = {
            'cc': 'collection',
            'wl': 'limit',
            'p': 'pattern',
            'f': 'field',
        }
        if 'filter' in self:
            for query_arg, filter_name in query_translator.items():
                if filter_name in self['filter']:
                    yield (query_arg, self['filter'][filter_name])

    @classmethod
    def from_json(cls, rule_json):
        """Create a Rule from json

        :param rule_json: a json representation of a Rule's dictionary
        :type  rule_json: str
        :returns: a single rule
        :rtype:   Rule instance

        :raises: ValueError
        """
        return cls(json.loads(rule_json))

    def to_json(self):
        """TODO: Docstring for to_json.
        :returns: a json representation of a Rule's dictionary
        :rtype:   str
        """
        return json.dumps(self)

    def validate(self, schema_files):
        """Validate a single YAML rule.

        :raises: pykwalify.errors.SchemaError
        """
        core = Core(source_data=self, schema_files=schema_files)
        core.validate(raise_exception=True)

    @property
    def is_batch(self):
        """Check if a rule uses a batch plugin.

        Lazy, trusts that we do not modify self['plugin'].
        :returns: whether a rule uses a batch plugin or not
        :rtype:   bool

        :raises: ImportError
        """
        if self._is_batch is not None:
            return self._is_batch
        plugin_module = import_module(self.pluginspec)
        return hasattr(plugin_module, 'pre_check')


class Rules(MutableSequence):
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
        """Return the rules that should run from comma-separated user input.

        :param user_choice: comma-separated list of requested rules
        :type  user_choice: str

        :returns: Rules
        :rtype:   seq
        """
        user_rules = set(user_choice.split(','))
        rules = cls()
        if ALL in user_rules:
            for filename, filepath in sorted(config_files.items()):
                rules.load_file(filepath)
            while ALL in user_rules:
                user_rules.remove(ALL)
        for user_rule in user_rules:
            rules.load_file(user_rule)
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
            for id_ in rule.requested_ids(user_ids):
                rule_json = json.dumps(rule)
                id_bundles[id_].append(rule_json)
        # Bundle into seq(json(rule)):ids
        bundles = defaultdict(list)
        for id_, rule_json in id_bundles.iteritems():
            bundles[tuple(rule_json)].append(id_)
        return bundles

    def load_file(self, filepath):
        """Load (and validate) a file of YAML documents of rules.

        :param filepath: file path of the file that needs to be loaded
        :type  filepath: str

        :raises: ArgumentTypeError
        """
        try:
            with open(filepath) as stream:
                for idx, document in enumerate(yaml.load_all(stream), 1):
                    document = Rule(document)
                    document.validate(schema_files.values())
                    self.append(document)
        except IOError as err:
            raise ArgumentTypeError("Cannot load rule file '{path}': {err}".
                                    format(path=filepath, err=err))

