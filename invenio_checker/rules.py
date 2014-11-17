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
import yaml
from argparse import ArgumentTypeError
from collections import defaultdict, MutableSequence
from pykwalify.core import Core
from pykwalify.errors import SchemaError

from .registry import config_files, schema_files
from .common import ALL


class DuplicateRuleError(Exception):
    pass


class Rule(dict):

    def __init__(self, *args, **kwargs):
        super(Rule, self).__init__(*args, **kwargs)

    @property
    def checkspec(self):
        """Resolve checkspec of the rule's check."""
        check = self['check']
        return 'invenio.modules.{module}.checkerext.plugins.{plugin}'\
            .format(module=check[0], plugin=check[1])

    def validate(self, schema_files):
        """Validate a single YAML rule.

        :raises: pykwalify.errors.SchemaError
        """
        core = Core(source_data=self, schema_files=schema_files)
        core.validate(raise_exception=True)


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

    def load_file(self, filepath):
        """Load (and validate) a file of YAML documents of rules.

        :param filepath: file path of the file that needs to be loaded
        :type  filepath: str:

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

