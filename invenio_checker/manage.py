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
from .common import ALL
from .ids import ids_from_input
from .plugins import Plugins
from .rules import Rules
from invenio.base.factory import create_app
from invenio.ext.script import Manager, change_command_name
from invenio.ext.sqlalchemy import db
from invenio.ext.sqlalchemy.utils import session_manager
from invenio.modules.workflows.models import BibWorkflowObject


class PluginMissing(Exception):
    def __init__(self, pluginspec, rule_name):
        message = "Could not find plugin `{0}` as defined in `{1}`"\
            .format(pluginspec, rule_name)
        super(PluginMissing, self).__init__(message)

manager = Manager()
plugins_dec = manager.option('--plugins', '-p', default=ALL, type=Plugins.from_input,
                             help='Set custom plugin files.')
rules_dec = manager.option('--rules', '-r', default=ALL, type=Rules.from_input,
                           help='Comma seperated list of rules to run, or ' + ALL)


@plugins_dec
@rules_dec
@manager.option('--ids', '-i', dest='user_ids', default=ALL, type=ids_from_input,
                help='List of record IDs to work on (overrides other filters),'
                ' or ' + ALL + ' to run on every single record')
@manager.option('--queue', '-q', default='Bibcheck',
                help='Specify the RT Queue in which tickets will be created')
@manager.option('--no-tickets', '-t', dest='tickets', action='store_false',
                help='Policy to create tickets by')
@manager.option('--no-upload', '-n', dest='upload', action='store_false',
                help='Disable uploading changes to the database')
@manager.option('--dry-run', '-d', action='store_true',
                help='Same as --no-tickets --no-upload')
def run(plugins, rules, user_ids, queue, tickets, upload, dry_run):
    """Initiate the execution of all requested rules."""
    # Preparations
    if dry_run:
        upload = False
        tickets = False

    # Ensure defined plugins exist
    for rule in rules:
        if rule.pluginspec not in plugins:
            raise PluginMissing((rule.pluginspec, rule['name']))

    # Run
    common = {
        'tickets': tickets,
        'queue': queue,
        'upload': upload
    }
    json_rulesets = rules.by_json_ruleset(user_ids)
    for rule_jsons, ids in json_rulesets.items():
        data = {
            'rule_jsons': rule_jsons,
            'ids': ids,
            'common': common
        }
        obj = BibWorkflowObject.create_object()
        obj.set_data(data)
        obj.save()
        obj.start_workflow("base_bundle", delayed=True)


@plugins_dec
@rules_dec
@change_command_name
def list_plugins(plugins, rules):
    """List all rules (and any associated plug-ins) and exit."""
    # TODO
    pass


def main():
    """Run manager."""
    manager.app = create_app()
    manager.run()


if __name__ == '__main__':
    main()
