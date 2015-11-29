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

"""Checker API."""

from invenio.ext.sqlalchemy import db  # pylint: disable=no-name-in-module,import-error
from json_import import json2models
from invenio_base.wrappers import lazy_import

CheckerRule = lazy_import('invenio_checker.models.CheckerRule')
CheckerReporter = lazy_import('invenio_checker.models.CheckerReporter')


def create_task(arguments, add=True, commit=True):
    """Create a new checker task.

    :param arguments: kwargs to pass to the database model
    :param add: add new task to sqlalchemy session
    :param commit: commit new task to sqlalchemy session

    :returns: new task
    """
    new_task = CheckerRule(**arguments)
    if add:
        db.session.add(new_task)

    if commit:
        db.session.commit()
    return new_task

def create_reporter(arguments, add=True, commit=True):
    """Create a new checker reporter.

    :param arguments: kwargs to pass to the database model
    :param add: add new reporter to sqlalchemy session
    :param commit: commit new reporter to sqlalchemy session

    :returns: new task
    """
    new_reporter = CheckerReporter(**arguments)
    if add:
        db.session.add(new_reporter)
        if commit:
            db.session.commit()
    return new_reporter

def remove_reporter(reporter, commit=True):
    """Remove a checker reporter from the database.

    :param reporter: reporter to remove
    :type reporter: invenio_checker.models.CheckerReporter

    :param commit: commit deletion of the reporter
    """
    db.session.delete(reporter)
    if commit:
        db.session.commit()

def edit_reporter(reporter, modifications, commit=True):
    """
    :param repoter: invenio_checker.models.CheckerReporter
    :param modifications: modifications to update the database with
    :param commit: commit modifications to the database
    """
    for key, value in modifications.iteritems():
        setattr(reporter, key, value)
    db.session.merge(reporter)
    if commit:
        db.session.commit()
    return reporter

def edit_task(current_task_name, modifications, commit=True):
    """Edit a task by intersecting it with given modifications.

    :param current_task_name: name targeted task currently has
    :param modifications: modifications to update the database with
    :param commit: commit modifications to the database
    """
    task = CheckerRule.query.filter(CheckerRule.name == current_task_name).one()
    for key, value in modifications.iteritems():
        setattr(task, key, value)
    db.session.merge(task)
    if commit:
        db.session.commit()
    return task

def delete_task(task_name):
    """Delete a task from the database.

    :param task_name: name of checker task to delete
    """
    task = CheckerRule.query.filter(CheckerRule.name == task_name).one()
    try:
        db.session.delete(task)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

def run_task(task_name, dry_run=False):
    """Run an existing task.

    :param task_name: name of task to delete
    :param dry_run: disable committing and reporting during executiong
    """
    from invenio_checker.clients.supervisor import run_task
    return run_task(task_name, dry_run=dry_run)

def import_task_from_json_file(json_file):
    """Import checker tasks from a json file.

    :param json_file: file path of json file to import from
    """
    db_objs = json2models(json_file, 'CheckerRule')
    try:
        db.session.add_all(db_objs)
    except Exception:
        db.session.rollback()
        raise

def branch_task(current_task_name, modifications, add=True, commit=True):
    """Create a checker task by using an existing one as a template.

    :param current_task_name: name of task to use as template
    :param modifications: changes to apply on the `current_tasks` settings
    :param add: add new reporter to sqlalchemy session
    :param commit: commit new reporter to sqlalchemy session
    """
    _task = CheckerRule.query.filter(CheckerRule.name == current_task_name).one()
    clone = _copy_row(_task)
    for key, value in modifications.iteritems():
        setattr(clone, key, value)
    if add:
        db.session.add(clone)
        if commit:
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise()
    return clone

def _copy_row(row, ignored_columns=frozenset()):
    """Copy a given database row.

    :param ignored_columns: column names to not copy
    """
    copy = row.__class__()

    for col in row.__table__.columns:
        if col.name not in ignored_columns:
            copy.__setattr__(col.name, getattr(row, col.name))

    return copy
