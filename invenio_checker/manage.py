# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2013, 2014, 2015 CERN.
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

"""Manage checker module."""

import inspect
from functools import wraps

from .common import ALL
from .recids import ids_from_input
from .registry import plugin_files
from .rules import Rules
from .models import CheckerRule
from invenio.base.factory import create_app
from invenio.ext.script import Manager, change_command_name
from .errors import PluginMissing

################################################################################
from invenio.ext.sqlalchemy import db
from .models import CheckerRule
from sqlalchemy.exc import IntegrityError
new_rule = CheckerRule(
    name='enum',
    plugin_module='invenio_checker',
    plugin_file='enum',
    option_holdingpen=True,
    option_consider_deleted_records=False,
    filter_pattern=None,
    filter_field=None,
    filter_limit=None,
)
try:
    old=CheckerRule.query.one()
    db.session.delete(old)
except:
    pass
try:
    db.session.add(new_rule)
    db.session.commit()
except IntegrityError:
    pass
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
        kwargs['rules'] = Rules.from_input(kwargs['rules'])
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
    # Ensure defined plugins exist
    for rule in rules:
        if rule.pluginspec not in plugin_files:
            raise PluginMissing((rule.pluginspec, rule['name']))

    # Run
    common = {
        'tickets': tickets,
        'queue': queue,
        'upload': upload
    }

    def getfile(modspec):
        """Resolve a modspec into the corresponding python file.

        :param modspec: dot-notated module specifier
        """
        path = inspect.getfile(plugin_files[modspec])
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    for rule in rules:
        from .tasks import run_test
        filepath = getfile(rule.pluginspec)

        # celery
        # z = run_test.delay(filepath, e='2cool')

        # celery sync
        z = run_test(filepath=filepath)

    # return bool(z)


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
