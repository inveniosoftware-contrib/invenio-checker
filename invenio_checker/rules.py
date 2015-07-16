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

"""Rule handlers for checker module."""
from collections import MutableSequence

from intbitset import intbitset  # pylint: disable=no-name-in-module
from invenio.legacy.search_engine import search_pattern

from .common import ALL
from .errors import DuplicateRuleError
from .models import CheckerRule


class Rules(MutableSequence):
    """Maintain a set of rules."""

    def __init__(self, *args):
        self.list = list()
        self.extend(list(args))

    def check(self, document):
        """Prevent rules with the same name from being added to the list.

        :raises: DuplicateRuleError
        """
        rule_name = document.name
        if rule_name in (rule.name for rule in self):
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
    def from_ids(cls, rule_ids):
        rules = cls()
        for rule_id in rule_ids:
            rules.load_rule(rule_id)
        return rules

    def load_rule(self, rule_name):
        """Load rules from the database.

        :param rule_name: rule name or ALL

        :raises: IndexError
        """
        if rule_name == ALL:
            for db_rule in CheckerRule.query.all():
                self.append(CheckerRule(db_rule))
        else:
            self.append(CheckerRule.from_name(rule_name))
