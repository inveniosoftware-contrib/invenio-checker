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


"""Manage checker module."""

import sys

from functools import wraps

from .common import ALL
from .recids import ids_from_input
from .registry import plugin_files
from .models import CheckerRule, CheckerRecord
from invenio.base.factory import create_app
from invenio.ext.script import Manager, change_command_name
from invenio.ext.sqlalchemy import db

################################################################################
# from invenio_checker.models import CheckerRule, CheckerRecord, CheckerReporter
# from invenio.ext.sqlalchemy import db
# CheckerRecord.query.delete()
# CheckerRule.query.delete()
# try:
#     print "1"
#     new_rule = CheckerRule(
#         name='enum',
#         plugin_module='invenio_checker',
#         plugin_file='enum',
#         option_holdingpen=True,
#         option_consider_deleted_records=False,
#         filter_pattern=None,
#         filter_records=None,
#     )
#     db.session.add(new_rule)
#     db.session.commit()
# except Exception:
#     pass
# try:
#     new_rule = CheckerRule(
#         name='enum2',
#         plugin_module='invenio_checker',
#         plugin_file='enum',
#         option_holdingpen=True,
#         option_consider_deleted_records=False,
#         filter_pattern=None,
#         filter_records=None,
#     )
#     db.session.add(new_rule)
#     db.session.commit()
# except Exception:
#     pass
################################################################################

manager = Manager(usage=__doc__)
rules_dec = manager.option('--rules', '-r', default=ALL,
                           help='Comma seperated list of rule names to load,'
                           ' or `{}` for all rules.'.format(ALL))


def interpret_dry_run(func):
    """Resolve `dry_run` to variables understood by `run()`."""
    @wraps(func)
    def _dry_run(*args, **kwargs):
        if 'dry_run' in kwargs:
            if kwargs['dry_run']:
                kwargs['upload'] = False
                kwargs['tickets'] = False
            del kwargs['dry_run']
        return func(*args, **kwargs)
    return _dry_run


def resolve_rules(func):
    """Resolve `rules` to list of Rules."""
    @wraps(func)
    def _resolve_rules(*args, **kwargs):
        kwargs['rules'] = CheckerRule.from_input(kwargs['rules'])
        return func(*args, **kwargs)
    return _resolve_rules


# TODO: --force check regardless of timestamp
@manager.option('--ids', '-i', dest='user_recids',
                default=ALL, type=ids_from_input,
                help='List of record IDs to work on (overrides other filters),'
                ' or `{}` to run on all records'.format(ALL))
@manager.option('--queue', '-q', default='Checker',
                help='Specify the RT Queue in which tickets will be created')
@manager.option('--no-tickets', '-t', dest='tickets', action='store_false',
                help='Policy to create tickets by')
@manager.option('--no-upload', '-n', dest='upload', action='store_false',
                help='Disable uploading changes to the database')
@manager.option('--dry-run', '-d', action='store_true',
                help='Same as --no-tickets --no-upload')
@rules_dec
@resolve_rules  # Must be anywhere after `rules`
@interpret_dry_run  # Must be after `dry_run`, `upload`, `tickets`
def run(rules, user_recids, queue, tickets, upload):
    """Initiate the execution of all requested rules.

    :param rules: rules to load
    :type  rules: list of rule_names or ALL

    :param user_recids: record IDs to consider
    :type  user_recids: intbitset

    :param queue: bibcatalog queue to create tickets in
    :type  queue: str

    :param tickets: whether to create tickets
    :type  tickets: bool

    :param upload: whether to upload amended records
    :type  upload: bool

    :returns: TODO
    :rtype:   TODO

    :raises: invenio.modules.checker.errors:PluginMissing
    """

    from .supervisor import run_task
    for rule in rules:
        run_task(rule.name)

    # return bool(z)


################################################################################

from json_import import json2models


@manager.option('--temporary', '-t', dest='temporary', action='store_true',
                help='Whether the new rule is a temporary one')
@manager.option('--filter-records', '-fr', dest='filter_records',
                help='')
@manager.option('--filter-pattern', '-fp', dest='filter_pattern',
                help='The filter pattern string')
@manager.option('--consider-deleted-records', '-cdr',
                dest='option_consider_deleted_records', action='store_true',
                help='Whether the new rule to consider deleted records')
@manager.option('--holding-pen', '-hp', dest='option_holdingpen',
                action='store_true',
                help='Holdingpen option')
@manager.option('--arguments', '-a', dest='arguments',
                help='Any arguments you want the new rule to have')
@manager.option('--plugin-file', '-pf', dest='plugin_file',
                help='The plugin file of the new rule')
@manager.option('--plugin-module', '-pm', dest='plugin_module',
                help='The plugin module of the new rule')
@manager.option('--name', '-n', dest='name',
                help='The name (id) of the new rule')
@change_command_name
def create_rule(**kwargs):
    """
    Creates a new rule.
    """
    new_rule = CheckerRule(**kwargs)
    db.session.add(new_rule)
    db.session.commit()


@manager.option('--temporary', '-t', dest='temporary', action='store_true',
                help='Whether the rule is a temporary one')
@manager.option('--filter-records', '-fr', dest='filter_records',
                help='')
@manager.option('--filter-pattern', '-fp', dest='filter_pattern',
                help='The filter pattern string')
@manager.option('--consider-deleted-records', '-cdr',
                dest='option_consider_deleted_records', action='store_true',
                help='Whether the rule to consider deleted records')
@manager.option('--holding-pen', '-hp', dest='option_holdingpen',
                action='store_true',
                help='Holdingpen option')
@manager.option('--arguments', '-a', dest='arguments',
                help='Any arguments you want the rule to have')
@manager.option('--plugin-file', '-pf', dest='plugin_file',
                help='The plugin file of the new rule')
@manager.option('--plugin-module', '-pm', dest='plugin_module',
                help='The plugin module of the rule')
@manager.option('--name', '-n', dest='name',
                help='The name (id) of the rule you want to edit')
@change_command_name
def edit_rule(rule_uuid, **updates):
    """
    Edits an existing rule.
    """
    print rule_uuid
    if 'name' in updates:
        updates.pop('name', None)
    rule = CheckerRule.query.filter(CheckerRule.name == rule_uuid).first()
    # Check keys and update as needed.
    for key, value in updates.iteritems():
        if not str.isalpha(key[0]):
            raise AttributeError(
                "Invalid update attribute specified: {}".format(key)
            )
        rule.__setattr__(key, value)
    db.session.commit()


@manager.option('--name', '-n', dest='rule_uuid',
                help='')
@change_command_name
def delete_rule(rule_uuid):
    """
    Deletes (and de-schedules) a rule.
    """
    rule = CheckerRule.query.filter(CheckerRule.name == rule_uuid).first()
    db.session.delete(rule)
    db.session.commit()


@manager.option('--name', '-n', dest='rule_uuid',
                help='')
@change_command_name
def display_rule(rule_uuid):
    """
    Displays (?) a rule.
    """
    rule = CheckerRule.query.filter(CheckerRule.name == rule_uuid).first()
    # print rule
    if rule is None:
        print >>sys.stderr,\
            "Sorry, no rule with id \"{}\" was found.".format(rule_uuid)
    else:
        print rule


def start_rules(*rule_uuids):
    """
    Starts a rule.
    """
    # from .supervisor import run_task
    # for rule_uuid in rule_uuids:
    #     run_task(rule_uuid)


def json_import(json_file):
    """
    Imports all models for the JSON files specified into the database.
    """
    # TODO: Fix hard-coded model name (CheckerRule)
    db_objs = json2models(json_file, 'CheckerRule')
    db.session.add_all(db_objs)


################################################################################

@rules_dec
@change_command_name
def list_plugins(rules):
    """List all rules (and any associated plug-ins) and exit."""
    # TODO (grouped by plugin, because they can be considered supersets)
    pass

def main():
    """Run manager."""
    manager.app = create_app()
    manager.run()

if __name__ == '__main__':
    main()
