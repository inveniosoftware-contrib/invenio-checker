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

from flask import Blueprint
from flask.json import jsonify
# from flask import request
from flask_breadcrumbs import register_breadcrumb
from flask_login import login_required

from invenio.base.decorators import templated
from invenio.base.i18n import _
from invenio.ext.principal import permission_required

from invenio_checker.models import CheckerRule
from invenio_checker.views.config import checker_rule_mapping

# from invenio.modules.access.local_config import \
# FIXME
WEBACCESSACTION = 'cfgwebaccess'

blueprint = Blueprint('checker_admin', __name__,
                      url_prefix="/admin/checker",
                      template_folder='../templates',
                      static_folder='../static')


@blueprint.route('/', methods=['GET', 'POST'])
@login_required
@permission_required(WEBACCESSACTION)
@templated('checker/admin/index.html')
@register_breadcrumb(blueprint, 'admin.checker_admin', _('Checker'))
def index():
    """Index."""
    pass


@blueprint.route('/api/tasks/get/header', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_tasks_header():
    """
    Returns a JSON representation of the CheckerRule schema.
    For security reasons, a list cannot be JSON-ified, so it has to be wrapped
    within a dictionary.
    """
    header = {"columns": {}}
    column_list = header["columns"]
    for mapping_key, mapping_value in checker_rule_mapping.items():
        if not mapping_value["hidden"]:
            column_list[mapping_key] = mapping_value
    return jsonify(header)


@blueprint.route('/api/tasks/get/data', methods=['POST'])
@login_required
@permission_required(WEBACCESSACTION)
def get_tasks_data():
    """
    Returns a JSON representation of the CheckerRule data.
    For security reasons, a list cannot be JSON-ified, so it has to be wrapped
    within a dictionary.
    """
    rows = {"rows": []}
    row_list = rows["rows"]
    rules = CheckerRule.query.all()
    for rule in rules:
        row_list.append(dict(rule))
    return jsonify(rows)


@blueprint.route('/create_task', methods=['GET', 'POST'])
@login_required
@permission_required(WEBACCESSACTION)
@templated('checker/admin/create_task.html')
@register_breadcrumb(blueprint, 'admin.checker_admin.create_task', _('Checker'))
def create_task():
    """Index."""
    pass