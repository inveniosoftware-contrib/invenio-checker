# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2012, 2013, 2014, 2015 CERN.
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

"""Checker Admin Flask Blueprint."""

from __future__ import unicode_literals
from importlib import import_module
from itertools import chain
from collections import defaultdict
from intbitset import intbitset

from six import reraise
from cerberus import Validator

from flask import (
    Blueprint,
    Response,
    redirect,
    request,
    stream_with_context,
    url_for,
    render_template_string,
)
from flask.json import jsonify
from flask_breadcrumbs import register_breadcrumb
from flask_login import login_required
from datetime import datetime

from wtforms import (  # pylint: disable=no-name-in-module
    Form,
    fields,
    validators,
    ValidationError,
)
from wtforms.widgets import TextInput
from wtforms.ext.appengine.db import model_form

from invenio.ext.sqlalchemy import db
from invenio.base.decorators import templated
from invenio.base.i18n import _
from invenio.ext.principal import permission_required

from invenio_checker.models import (
    CheckerRule,
    CheckerRuleExecution,
)
from invenio_checker.views.config import (
    task_mapping,
    check_mapping,
    log_mapping,
)

from ..registry import plugin_files

from ..common import ALL
from ..recids import ids_from_input
import sys
from croniter import croniter

# from invenio.modules.access.local_config import \
# FIXME
WEBACCESSACTION = 'cfgwebaccess'

blueprint = Blueprint('checker_admin', __name__,
                      url_prefix="/admin/checker",
                      template_folder='../templates',
                      static_folder='../static')

def get_NewTaskForm(*args, **kwargs):

    from ..models import SendEmail

    class NewTaskForm(Form):
        name = fields.StringField(
            'Task name',
            validators=[validators.InputRequired()],
        )
        plugin = fields.SelectField(
            'Plugin',
            choices=[(plugin, plugin) for plugin in plugin_files],
            validators=[validators.InputRequired()],
        )
        send_email = fields.SelectField(
            'Send email',
            choices=[
                ('on_failure', 'On failure'),
                ('always', 'Always'),
                ('never', 'Never'),
            ],
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
        periodic = fields.BooleanField(
            'Run this rule periodically',
        )
        schedule = fields.StringField(
            'Schedule',
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
                    reraise(ValidationError, evalue, etb)

        def validate_schedule(self, field):
            """Ensure that `schedule` is accepted by `croniter`."""
            if not field.data:
                return
            try:
                croniter(field.data)
            except Exception:
                # May be TypeError/KeyError/AttributeError, who knows what else
                # Let's play it safe.
                reraise(ValidationError, *sys.exc_info()[1:])

        def validate_send_email(self, field):
            field.data = SendEmail[field.data]

    return NewTaskForm(*args, **kwargs)


@blueprint.route('/')
def index():
    """Redirect to the tasks view."""
    return redirect(url_for('.view', page_name='tasks'))


@blueprint.route('/view/<page_name>')
@login_required
@permission_required(WEBACCESSACTION)
@templated('checker/admin/index.html')
@register_breadcrumb(blueprint, 'admin.checker_admin', _('Checker'))
def view(page_name):
    """Have javascript load the correct page."""
    return {
        'page_name': page_name,
        'new_task_form': get_NewTaskForm()
    }

# Tasks

@blueprint.route('/api/tasks/get/header', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_tasks_header():
    """
    Return a JSON representation of the CheckerRule schema.

    For security reasons, a list cannot be JSON-ified, so it has to be wrapped
    within a dictionary.
    """
    header = {"cols": {}}
    column_list = header["cols"]
    for mapping_key, mapping_value in task_mapping.items():
        column_list[mapping_key] = mapping_value
    return jsonify(header)


@blueprint.route('/api/tasks/get/data', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_tasks_data():
    """
    Return a JSON representation of the CheckerRule data.

    For security reasons, a list cannot be JSON-ified, so it has to be wrapped
    within a dictionary.
    """
    rows = {"rows": []}
    row_list = rows["rows"]
    rules = CheckerRule.query.all()
    for rule in rules:
        rule_d = rule.__dict__  # Work around inveniosoftware/invenio-ext#16
        rule_d = {key: val for key, val in rule_d.items()
                  if not key.startswith('_')}
        rule_d['arguments'] = str(rule_d['arguments'])
        rule_d['plugin'] = rule.plugin,
        if rule_d['filter_records'].is_infinite():
            rule_d['filter_records'] = '' # Infinite is not serializable
        else:
            rule_d['filter_records'] = list(rule_d['filter_records'])
        rule_d['send_email'] = rule_d['send_email'].name
        row_list.append(rule_d)
    return jsonify(rows)


# Checks

@blueprint.route('/api/checks/get/header', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_checks_header():
    """Returns the header of the checks table."""
    return jsonify({"cols": check_mapping})


@blueprint.route('/api/checks/get/data', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_checks_data():
    """Returns the columns of the checks table."""
    checks = []
    for name, plugin in plugin_files.items():
        checks.append({
            'name': name,
            'description': plugin.__doc__,
        })
    return jsonify({"rows": checks})


@blueprint.route('/api/checks/stream_check/<pluginspec>', methods=['GET'])
@login_required
@permission_required(WEBACCESSACTION)  # FIXME: Admin permission?
def stream_check(pluginspec):
    """
    TODO
    """
    def read_file():
        filepath = plugin_files[pluginspec].__file__
        with open(filepath, 'r') as file_:
            for line in file_:
                yield line
    return Response(stream_with_context(read_file()))


# executions

@blueprint.route('/api/executions/get/header', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_logs_header():
    """Returns the header of the checks table."""
    return jsonify({"cols": log_mapping})


@blueprint.route('/api/executions/get/data', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
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
    return jsonify({"rows": loglist})


@blueprint.route('/api/executions/stream_structured/<uuid>', methods=['GET'])
@login_required
@permission_required(WEBACCESSACTION)
def stream_logs(uuid):
    """Returns the columns of the checks table."""
    execution = CheckerRuleExecution.query.get(uuid)
    return Response(stream_with_context(execution.read_logs()))


# Create

@blueprint.route('/api/create_task/get_arguments_spec/<plugin_name>', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
@templated('checker/admin/create_task.html')
def get_arguments_spec(plugin_name):
    """Return complementary form fields for a check's arguments."""
    form = get_ArgForm(plugin_name)
    return render_template_string('''
    {% import 'checker/admin/macros_bootstrap.html' as bt %}
    {{ bt.render_form(form, nested=True) }}
    ''', form=form)


def get_schema_for_plugin(plugin_name):
    if plugin_name not in plugin_files:
        raise Exception(plugin_name)  # TODO
    module = import_module(plugin_name)
    try:
        return module.argument_schema
    except AttributeError:
        return {}


from functools import partial
from invenio.utils import forms

type_to_wtforms = {
    "string": partial(fields.StringField, validators=[validators.InputRequired()]),
    "text": partial(fields.TextAreaField, validators=[validators.InputRequired()]),
    "integer": partial(fields.IntegerField, validators=[validators.InputRequired()]),
    "float": partial(fields.DecimalField, validators=[validators.InputRequired()]),
    "decimal": partial(fields.DecimalField, validators=[validators.InputRequired()]),
    "boolean": fields.BooleanField,

    "datetime": partial(fields.DateTimeField, widget=forms.DateTimePickerWidget()),
    "date": partial(fields.DateField, widget=forms.DatePickerWidget()),
}


def get_ArgForm(plugin_name, *args, **kwargs):
    """Get a WTForms form based on the cerberus schema defined in the check."""

    class ArgForm(Form):
        """Empty form to populate based on cerberus schema."""

    arguments_schema = {"arg_" + k: v for k, v
                        in get_schema_for_plugin(plugin_name).items()}
    for key, spec in arguments_schema.items():
        setattr(
            ArgForm,
            key,
            type_to_wtforms[spec['type']]()
        )

    return ArgForm(*args, **kwargs)


@blueprint.route('/api/create_task/submit', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
@templated('checker/admin/create_task.html')
def submit_task():
    form_origin = get_NewTaskForm(request.form)
    form_plugin = get_ArgForm(request.form['plugin'], request.form)

    from celery.contrib import rdb; rdb.set_trace()
    if form_origin.validate() & form_plugin.validate():
        form_for_db = form_origin.data.copy()  # pylint: disable=no-member
        periodic = form_for_db.pop('periodic', False)
        if not periodic:
            form_for_db['schedule'] = None
        form_for_db['last_run'] = datetime.now()
        try:
            rule = CheckerRule(
                arguments=form_plugin.data,  # pylint: disable=no-member
                **form_for_db  # pylint: disable=no-member
            )
            db.session.add(rule)
            db.session.commit()
        except Exception as e:
            return {'success': False,
                    'failure_type': 'commit',
                    'errors': str(e)}
        return {'success': True}
    else:
        all_errors = defaultdict(list)
        for field, errors in chain(
                form_origin.errors.items(),  # pylint: disable=no-member
                form_plugin.errors.items(),  # pylint: disable=no-member
        ):
            all_errors[field].extend(errors)
        return {'success': False,
                'failure_type': 'validation',
                'errors': all_errors}


@blueprint.route('/translate', methods=['GET'])
@login_required
@permission_required(WEBACCESSACTION)
def translate():
    """Returns the columns of the checks table."""
    from invenio.base.i18n import _
    return str(_(request.args['english']))
