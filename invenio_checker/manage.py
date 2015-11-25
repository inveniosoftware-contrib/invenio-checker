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
from functools import partial

from functools import wraps, update_wrapper

from .common import ALL
from .recids import ids_from_input
from .registry import plugin_files
from .models import CheckerRule, CheckerRecord
from invenio_base.factory import create_app
from invenio.ext.script import Manager, change_command_name
from flask.ext.script import (  # pylint: disable=no-name-in-module,import-error
    Command,
    Option,
)
from .api import (
    create_task,
    edit_task,
    delete_task,
    run_task,
    import_task_from_json_file,
    branch_task,
)


manager = Manager(usage=__doc__)

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

# def resolve_rules(func):
#     """Resolve `rules` to list of Rules."""
#     @wraps(func)
#     def _resolve_rules(*args, **kwargs):
#         kwargs['rules'] = CheckerRule.from_input(kwargs['rules'])
#         return func(*args, **kwargs)
#     return _resolve_rules

# TODO: --force check regardless of timestamp
# TODO: reporters

Option = partial(Option, default=None)

opt_task_name_pos = Option(
    'task_name_pos', metavar='task_name_pos',
    help='The task name'
)
opt_task_name_pos_multi = Option(
    'task_name_pos_multi', metavar='task_name_pos_multi', nargs='+',
    help='Task names to act upon'
)
opt_task_name_pos_multi_maybe = Option(
    'task_name_pos_multi_maybe', metavar='task_name_pos_multi_maybe', nargs='*',
    help='Task names to act upon'
)
opt_task_name = Option(
    '--task-name', '-n', dest='task_name',
    help='The task name'
)

# opt_no_reporters = Option(
#     '--no-reporters', '-R', dest='reporters_enabled', action='store_true',
#     help='Disable reporters'
# )
# opt_reports = Option(
#     '--reports', '-r', dest='reports_disabled', action='store_false',
#     help='Enable reporters'
# )

opt_dry_run = Option(
    '--dry-run', '-d', dest='dry_run', action='store_true',
    help='Disable committing changes to the database and reporting'
)

opt_temporary = Option(
    '--temporary', '-t1', dest='temporary', action='store_true',
    help='Task should be temporary'
)
opt_no_temporary = Option(
    '--no-temporary', '-t0', dest='temporary', action='store_false',
    help='Task should not be temporary (default)'
)

opt_filter_records = Option(
    '--filter-records', '-fr', dest='filter_records', type=ids_from_input,
    help='Records IDs to run on. Intersected with --filter-pattern'
)
opt_filter_pattern = Option(
    '--filter-pattern', '-fp', dest='filter_pattern',
    help='The filter pattern string. Intersected with --filter-records'
)

opt_consider_deleted_records = Option(
    '--consider-deleted-records', '-cdr1',
    dest='consider_deleted_records', action='store_true',
    help='Do not consider deleted records in search'
)
opt_no_consider_deleted_records = Option(
    '--no-consider-deleted-records', '-cdr0',
    dest='consider_deleted_records', action='store_false',
    help='Consider deleted records in search'
)

opt_arguments = Option(
    '--arguments', '-a', dest='arguments',
    help='Arguments that will be passed to the task'
)

opt_check_spec = Option(
    '--check-spec', '-c', dest='plugin',
    help='The check import specifier (eg invenio_thing.foo.bar.test_baz)'
)
opt_check_spec_pos = Option(
    'plugin', metavar='plugin',
    help='The check import specifier (eg invenio_thing.foo.bar.test_baz)'
)

opt_schedule = Option(
    '--schedule', '-s', dest='schedule', type=lambda x: None if not(x) else x,
    help='Schedule that the task will have (cronjob format)'
)
opt_schedule_enable = Option(
    '--schedule-enable', '-s1', dest='schedule_enabled', action='store_true',
    help='Enable the schedule (Default)',
)
opt_schedule_disable = Option(
    '--schedule-disable', '-s0', dest='schedule_enabled', action='store_false',
    help='Disable the schedule',
)

# opt_dry_run = Option( # TODO
#     '--dry-run', '-d', action='store_true',
#     help='Same as --no-reports --no-commit'
# )


class CreateTask(Command):

    option_list = (
        opt_task_name_pos, opt_check_spec_pos,
        opt_no_temporary, opt_temporary,
        opt_filter_records, opt_filter_pattern,
        opt_consider_deleted_records, opt_no_consider_deleted_records,
        opt_arguments,
        opt_schedule, opt_schedule_disable, opt_schedule_enable,
    )

    def run(self, **options):
        """
        Creates a new rule.
        """
        options['name'] = options.pop('task_name_pos')
        create_task(options)

manager.add_command('create-task', CreateTask())


class BranchTask(Command):

    option_list = (
        opt_task_name_pos,
        opt_task_name,
        opt_check_spec,
        opt_no_temporary, opt_temporary,
        opt_filter_records, opt_filter_pattern,
        opt_consider_deleted_records, opt_no_consider_deleted_records,
        opt_arguments,
        opt_schedule, opt_schedule_disable, opt_schedule_enable,
    )

    def run(self, **kwargs):
        """
        Branch from an existing task
        """
        blueprint_task_name = kwargs.pop('task_name_pos')
        kwargs['name'] = kwargs.pop('task_name', None)
        modifications = {key: val for key, val in kwargs.items()
                         if val is not None}

        branch_task(blueprint_task_name, modifications)


manager.add_command('branch-task', BranchTask())


class ModifyTask(Command):

    option_list = (
        opt_task_name_pos,
        opt_task_name,
        opt_check_spec,
        opt_no_temporary, opt_temporary,
        opt_filter_records, opt_filter_pattern,
        opt_consider_deleted_records, opt_no_consider_deleted_records,
        opt_arguments,
        opt_schedule, opt_schedule_disable, opt_schedule_enable,
    )

    def run(self, **kwargs):
        """
        Edit an existing task.
        """
        current_task_name = kwargs.pop('task_name_pos')
        kwargs['task_name'] = kwargs.pop('task_name', None)
        modifications = {key: val for key, val in kwargs.items()
                         if val is not None}

        edit_task(current_task_name, modifications)


manager.add_command('modify-task', ModifyTask())


class DeleteTask(Command):

    option_list = (
        opt_task_name_pos_multi,
    )

    def run(self, task_name_pos_multi):
        """
        Deletes given tasks.
        """
        for task_name_pos in task_name_pos_multi:
            delete_task(task_name_pos)


manager.add_command('delete-task', DeleteTask())


class RunTask(Command):

    # TODO: Dry run
    option_list = (
        opt_task_name_pos_multi,
        opt_dry_run,
    )

    def run(self, task_name_pos_multi, dry_run):
        for task_name_pos in task_name_pos_multi:
            run_task(task_name_pos, dry_run=dry_run)


manager.add_command('run-task', RunTask())


class ShowTask(Command):

    option_list = (
        opt_task_name_pos_multi_maybe,
    )

    def run(self, task_name_pos_multi_maybe):
        """
        Displays (?) a rule.
        """
        if not task_name_pos_multi_maybe:
            for rule in CheckerRule.query.all():
                print rule
        else:
            for rule_name in task_name_pos_multi_maybe:
                print CheckerRule.query.filter(CheckerRule.name == rule_name).first()


manager.add_command('show-task', ShowTask())


class ImportTask(Command):

    option_list = (
        Option(
            'json_files', nargs='+',
            help='JSON files to import from'
        ),
    )

    def run(self, json_files):
        """
        Imports all models for the JSON files specified into the database.
        """
        for json_file in json_files:
            import_task_from_json_file(json_file)


manager.add_command('import-task', ImportTask())


# @change_command_name
# def list_plugins():
#     """List all rules (and any associated plug-ins) and exit."""
#     # TODO (grouped by plugin, because they can be considered supersets)
#     pass
