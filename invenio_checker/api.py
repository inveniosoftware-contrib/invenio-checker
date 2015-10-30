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

from .models import (
    CheckerRule,
    CheckerReporter,
)
from invenio.ext.sqlalchemy import db
from json_import import json2models


def create_task(arguments, add=True, commit=True):
    """
    :param options: kwargs to pass to the database model
    """
    new_task = CheckerRule(**arguments)
    if add:
        db.session.add(new_task)
        if commit:
            db.session.commit()
    return new_task

def create_reporter(arguments, attach_to_tasks=None, add=True, commit=True):
    """
    :param attach_to_tasks: tasks to attach this reporter to
    :type attach_to_tasks: tuple
    """
    new_reporter = CheckerReporter(**arguments)

    for task in attach_to_tasks:
        task.reporters.append(new_reporter)
    if add:
        db.session.add(new_reporter)
        if commit:
            db.session.commit()

    return new_reporter

def edit_task(current_task_name, modifications, add=True, commit=True):
    """
    :param current_task_name: name targeted task currently has
    :param modifications: modifications to update the database with
    """
    task = CheckerRule.query.filter(CheckerRule.name == current_task_name).one()
    for key, value in modifications.iteritems():
        setattr(task, key, value)
    if commit:
        db.session.commit()
    return task

def delete_task(task_name):
    """
    :param task_name: name of task to delete
    """
    task = CheckerRule.query.filter(CheckerRule.name == task_name).first()
    try:
        db.session.delete(task)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

def run_task(task_name):
    """
    :param task_name: name of task to delete
    """
    from .supervisor import run_task
    run_task(task_name)

def import_task_from_json_file(json_file):
    """
    :param json_file: file path of json file to import from
    """
    db_objs = json2models(json_file, 'CheckerRule')
    db.session.add_all(db_objs)
