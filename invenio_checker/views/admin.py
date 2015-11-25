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

"""Checker Admin Flask Views."""

from __future__ import unicode_literals

import json
import sys
from collections import defaultdict
from functools import partial
from itertools import chain
from traceback import format_exc

import six
from croniter import croniter
from flask import Blueprint, Response, redirect, render_template, \
    render_template_string, request, stream_with_context, url_for
from flask.json import jsonify
from flask_breadcrumbs import register_breadcrumb
from flask_login import login_required
from intbitset import intbitset  # pylint: disable=no-name-in-module
from wtforms import Form, \
    ValidationError, fields, validators  # pylint: disable=no-name-in-module

from invenio_base.decorators import templated
from invenio_base.i18n import _
from invenio_base.wrappers import lazy_import
from invenio_ext.principal import permission_required
from invenio_ext.sqlalchemy import db
from invenio.utils import forms
from invenio_checker.api import create_task, delete_task, edit_task, run_task, create_reporter, edit_reporter, remove_reporter

from invenio_checker.acl import modifychecker, viewchecker
from ..recids import ids_from_input
from ..registry import plugin_files, reporters_files
from ..views.config import check_mapping, log_mapping, task_mapping
from invenio_checker.models import get_reporter_db
from sqlalchemy.orm.exc import NoResultFound

CheckerRule = lazy_import('invenio_checker.models.CheckerRule')
CheckerRuleExecution = \
    lazy_import('invenio_checker.models.CheckerRuleExecution')
CheckerReporter = lazy_import('invenio_checker.models.CheckerReporter')
default_date = lazy_import('invenio_checker.models.default_date')
ranges_str = lazy_import('invenio_checker.models.ranges_str')

blueprint = Blueprint(
    'invenio_checker_admin',
    __name__,
    url_prefix="/admin/checker",
    template_folder='../templates',
    static_folder='../static'
)


def get_NewTaskForm(*args, **kwargs):

    class NewTaskForm(Form):
        name = fields.StringField(
            'Task name',
            validators=[validators.InputRequired()],
        )
        plugin = fields.SelectField(
            'Check',
            choices=[(plugin, plugin) for plugin in plugin_files],
            validators=[validators.InputRequired()],
        )
        reporters = fields.SelectMultipleField(
            'Reporters',
            choices=[(reporter, reporter) for reporter in reporters_files],
        )
        consider_deleted_records = fields.BooleanField(
            'Consider deleted records',
        )
        force_run_on_unmodified_records = fields.BooleanField(
            'Force run on unmodified records',
        )
        filter_pattern = fields.StringField(
            'Search pattern',
        )
        filter_records = fields.StringField(
            'Record IDs',
        )
        schedule = fields.StringField(
            'Schedule',
        )
        requested_action = fields.SelectField(
            'Requested action',
            choices=[
                ('submit_save',) * 2,
                ('submit_run_and_schedule',) * 2,
                ('submit_schedule',) * 2,
                ('submit_run',) * 2,
            ]
        )
        confirm_hash_on_commit = fields.BooleanField(
            'Ensure record hash did not change during execution',
        )
        allow_chunking = fields.BooleanField(
            'Allow chunking this task to multiple workers',
            default=True,
        )
        # Hidden
        schedule_enabled = fields.BooleanField(
            'Run this rule periodically',
        )
        modify = fields.BooleanField(
            'Request modification instead of creation',
        )
        original_name = fields.StringField(
            'Original name for modification',
        )

        def validate_filter_records(self, field):
            """Ensure that `filter_records` can be parsed by intbitset."""
            if not field.data:
                field.data = intbitset(trailing_bits=True)
            else:
                try:
                    field.data = ids_from_input(field.data)
                except TypeError:
                    etype, evalue, etb = sys.exc_info()
                    six.reraise(ValidationError, evalue, etb)

        def validate_schedule(self, field):
            """Ensure that `schedule` is accepted by `croniter`."""
            if not field.data:
                return
            try:
                croniter(field.data)
            except Exception:
                # May be TypeError/KeyError/AttributeError, who knows what else
                # Let's play it safe.
                six.reraise(ValidationError, *sys.exc_info()[1:])

    return NewTaskForm(*args, **kwargs)


@blueprint.route('/')
def index():
    """Redirect to the tasks view."""
    return redirect(url_for('.view', page_name='tasks'))

@blueprint.route('/api/records/get')
@login_required
@permission_required(viewchecker.name)
def record_brief():
    """XXX Temporary function to showcase live search."""
    from invenio_search.api import Query
    records = Query(request.args['query']).search().records()[:5]
    return ''.join(
        render_template('format/record/Default_HTML_brief.tpl', record=i)
        for i in records)

@blueprint.route('/view/<page_name>')
@login_required
@permission_required(viewchecker.name)
@templated('checker/admin/index.html')
@register_breadcrumb(blueprint, 'admin.checker_admin', _('Checker'))
def view(page_name):
    """Have javascript load the correct page."""
    if page_name in ('task_modify', 'task_branch'):
        # These are only supported by via javascript at the moment
        return redirect(url_for('.view', page_name='task_create'))
    return {'page_name': page_name}

@blueprint.route('/render/new_task_form')
@login_required
@permission_required(viewchecker.name)
def new_task_form():
    return render_template('checker/admin/task_form.html',
                           new_task_form=get_NewTaskForm())

# Tasks
@blueprint.route('/api/tasks/get/data', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def get_all_tasks_data():
    """Return a JSON representation of the CheckerRule data."""
    columns = {}
    for mapping_key, mapping_value in task_mapping.items():
        columns[mapping_key] = mapping_value
    rows = []
    for rule in CheckerRule.query.all():
        rows.append(get_task_data(rule))
    return jsonify({'rows': rows, 'cols': columns})

@blueprint.route('/api/tasks/get/data/<task_name>', methods=['POST'])
@login_required
@permission_required(viewchecker.name)
def get_single_task(task_name):
    """Return the data for a single task."""
    return jsonify(get_task_data(CheckerRule.query.filter(CheckerRule.name==task_name).one()))

def get_task_data(rule):
    """Extract a serializable dict for a rule."""
    # Extract all the fields
    rule_d = rule.__dict__  # Work around inveniosoftware/invenio-ext#16
    rule_d = {key: val for key, val in rule_d.items()
              if not key.startswith('_')}
    # Watable expects integers instead of booleans
    for key, val in rule_d.items():
        if isinstance(val, bool):
            rule_d[key] = int(val)
    # Filter out underscored keys
    rule_d = {key: val for key, val in rule_d.items()
              if not key.startswith('_')}
    # Serialize unserializable things
    rule_d['arguments'] = json.dumps(rule_d['arguments'], default=default_date)
    rule_d['filter_records'] = ranges_str(rule_d['filter_records'])
    rule_d['reporters'] = [rep.plugin for rep in rule.reporters]
    return rule_d

# Checks
@blueprint.route('/api/checks/get/data', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def get_checks_data():
    """Returns the columns of the checks table."""
    checks = []
    for name, plugin in plugin_files.items():
        checks.append({
            'name': name,
            'description': plugin.__doc__,
        })
    return jsonify({'rows': checks, 'cols': check_mapping})

@blueprint.route('/api/checks/stream_check/<pluginspec>', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def stream_check(pluginspec):
    """Stream the raw contents of a check file."""
    def read_file():
        filepath = plugin_files[pluginspec].__file__
        with open(filepath, 'r') as file_:
            for line in file_:
                yield line
    return Response(stream_with_context(read_file()))

# executions
@blueprint.route('/api/executions/get/data', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def get_logs_data():
    """Returns the columns of the checks table."""
    loglist = []
    for execution in CheckerRuleExecution.query.all():
        loglist.append({
            'task':
            execution.rule.name,

            'start_date':
            int(execution.start_date.strftime("%s")) * 1000
            if execution.start_date else None,

            'status_update_date':
            int(execution.status_update_date.strftime("%s")) * 1000
            if execution.status_update_date else None,

            'status':
            str(execution.status.name),

            'owner':
            execution.owner.nickname
            if execution.owner else '',

            'owner_id':
            execution.owner.id
            if execution.owner else -1,

            'uuid':
            execution.uuid,
        })
    return jsonify({"rows": loglist, "cols": log_mapping})

@blueprint.route('/api/executions/stream_structured/<uuid>', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def stream_logs(uuid):
    """Returns the columns of the checks table."""
    execution = CheckerRuleExecution.query.get(uuid)
    return Response(stream_with_context(execution.read_logs()))

@blueprint.route('/api/task_create/get_arguments_spec', methods=['POST'])
@login_required
@permission_required(viewchecker.name)
def task_create():
    """Return complementary form fields for a check's arguments."""
    plugin_name = request.form['plugin_name']
    form = get_ArgForm(plugin_name)
    task_name = request.form.get('task_name')

    # If a `task_name` was sent in the request, then an already-existing's
    # plugin's contents should be filled-in. (ie editing an existing task)
    if task_name:
        if plugin_name in plugin_files:
            task = CheckerRule.query.get(task_name)
        elif plugin_name in reporters_files:
            task = CheckerReporter.query.filter(
                CheckerReporter.rule_name == task_name,
                CheckerReporter.plugin == plugin_name).first()
        # The query will only have returned something if the specified
        # task_name already has a relationship with a `plugin_name` type
        # reporter.
        if task:
            for key, val in task.arguments.items():
                key = "arg_"+plugin_name+"_"+key
                field = getattr(form, key)
                # For choice fields, the field requires that the `data` is
                # `str(idx_of_selection)`
                if hasattr(field, 'choices'):
                    if isinstance(val, six.text_type): # Single choice
                        val = next((str(idx) for idx, choice in field.choices
                                    if choice == val))
                    else:
                        # Multi choice
                        # UNTESTED XXX
                        val = [str(idx) for idx, choice in field.choices
                               if choice in val]
                setattr(field, 'data', val)

    return render_template_string('''
    {% import 'checker/admin/macros_bootstrap.html' as bt %}
    {{ bt.render_form(form, nested=True) }}
    ''', form=form)

def get_ArgForm(plugin_name, *args, **kwargs):
    """Get a WTForms form based on the cerberus schema defined in the check."""

    class ArgForm(Form):
        """Empty form to populate based on plugin-provided schema."""

        @property
        def data_for_db(self):
            ret = {}
            for key, val in self.data.items():  # pylint: disable=no-member
                stripped_key = key[len(ArgForm.prefix+"_"):]
                field = getattr(self, key)
                schema = ArgForm.schema
                # Don't change the order of the checks here:
                # SelectMultipleField inherits from SelectField
                if isinstance(field, fields.SelectMultipleField):
                    val = [schema[stripped_key]['values'][int(itm)]
                           for itm in val]
                elif isinstance(field, fields.SelectField):
                    val = schema[stripped_key]['values'][int(val)]
                ret[stripped_key] = val
            return ret

    type_to_wtforms = {
        "string": partial(fields.StringField, validators=[validators.InputRequired()]),
        "text": partial(fields.TextAreaField, validators=[validators.InputRequired()]),
        "integer": partial(fields.IntegerField, validators=[validators.InputRequired()]),
        "float": partial(fields.DecimalField, validators=[validators.InputRequired()]),
        "decimal": partial(fields.DecimalField, validators=[validators.InputRequired()]),
        "boolean": partial(fields.BooleanField, validators=[]),

        "choice": partial(fields.SelectField, validators=[validators.InputRequired()]),
        "choice_multi": partial(fields.SelectMultipleField, validators=[]),

        "datetime": partial(fields.DateTimeField, widget=forms.DateTimePickerWidget()),
        "date": partial(fields.DateField, widget=forms.DatePickerWidget()),
    }

    def get_schema(requested_plugin_name):
        all_plugins = dict(chain(plugin_files.items(), reporters_files.items()))
        try:
            return all_plugins[requested_plugin_name].argument_schema
        except AttributeError:
            return {}

    def build_choices(schema, type_name, key):
        if type_name in ('choice', 'choice_multi'):
            ret = {'choices': []}
            for idx, choice in enumerate(schema[key]['values']):
                ret['choices'].append((idx, choice))
            return ret
        else:
            return {}

    # FIXME: These names might create collisions?
    ArgForm.plugin_name = plugin_name
    ArgForm.prefix = "arg_{}".format(plugin_name)
    ArgForm.schema = get_schema(plugin_name)
    for key, spec in ArgForm.schema.items():
        type_name = spec['type']
        type_kwargs = build_choices(ArgForm.schema, type_name, key)
        try:
            type_kwargs['label'] = spec['label']
        except KeyError:
            type_kwargs['label'] = key
        prefixed_key = "{}_{}".format(ArgForm.prefix, key)
        setattr(ArgForm, prefixed_key, type_to_wtforms[type_name](**type_kwargs))

    return ArgForm(*args, **kwargs)

@blueprint.route('/api/task_create/submit', methods=['POST'])
@login_required
@permission_required(viewchecker.name)
def submit_task():
    """Insert or modify an existing task and its reporters."""
    from invenio_checker.clients.supervisor import run_task

    def failure(type_, errors):
        assert type_ in ('general', 'validation')
        return jsonify({'failure_type': type_, 'errors': errors}), 400

    def success():
        return jsonify({})

    # Recreate the forms that we have previously served to the user so that we
    # can validate.
    form_origin = get_NewTaskForm(request.form)
    form_plugin = get_ArgForm(request.form['plugin'], request.form)
    if not (form_origin.validate() & form_plugin.validate()):
        form_errors = defaultdict(list)
        for field, errors in chain(form_origin.errors.items(),
                                   form_plugin.errors.items()):
            form_errors[field].extend(errors)
        return failure('validation', form_errors)

    # Get a dictionary that we can pass as kwargs to the database object,
    form_for_db = dict(form_origin.data)
    # but first, pop metadata out of it.
    modify = form_for_db.pop('modify')
    original_name = form_for_db.pop('original_name')
    requested_action = form_for_db.pop('requested_action')
    reporter_names = form_for_db.pop('reporters')
    form_for_db['arguments'] = form_plugin.data_for_db

    try:
        if modify:
            task = edit_task(original_name, form_for_db, commit=False)
        else:
            task = create_task(form_for_db, commit=False)
        # Add reporters
        for reporter_name in reporter_names:
            form_reporter = get_ArgForm(reporter_name, request.form)
            create_reporter(
                {'plugin': form_reporter.plugin_name,
                 'arguments': form_reporter.data_for_db},
                attach_to_tasks=(task,),
                commit=False
            )
            db.session.commit()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return failure('general', (str(e),))

    if requested_action.startswith('submit_run'):
        try:
            run_task(task.name)
        except Exception as e:
            return failure('general', (str(e),))

    return success()

@blueprint.route('/translate', methods=['GET'])
@login_required
@permission_required(viewchecker.name)
def translate():
    """Returns the columns of the checks table."""
    from invenio_base.i18n import _
    return str(_(request.args['english']))

@blueprint.route('/api/task_run', methods=['GET'])
@login_required
@permission_required(modifychecker.name)
def task_run():
    requested_task_names = request.values.getlist('task_names[]')
    tasks_in_db = CheckerRule.query.filter(
        CheckerRule.name.in_(requested_task_names)).all()
    if len(tasks_in_db) != len(requested_task_names):
        return jsonify({'error': 'Missing tasks requested'}), 400
    for task in tasks_in_db:
        run_task(task.name)
    return jsonify({})

@blueprint.route('/api/task_delete', methods=['GET'])
@login_required
@permission_required(modifychecker.name)
def task_delete():
    # TODO: Instead of deleting, MARK as deleted so that we can keep executions
    # around
    task_names = request.values.getlist('task_names[]')
    for task_name in task_names:
        delete_task(task_name)
    return jsonify({})
