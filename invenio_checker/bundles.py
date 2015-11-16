# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
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

"""Checker bundles."""

from __future__ import unicode_literals

from invenio_base.bundles import invenio as _i, jquery as _j
from invenio.ext.assets import Bundle, CleanCSSFilter, RequireJSFilter


js = Bundle(
    'js/checker/admin/init.js',
    filters=RequireJSFilter(exclude=[_j, _i]),
    output='checker_task_view.js',
    weight=30,
    bower={
        "jquery": "latest",
        "bootstrap-datepicker-eyecon": "latest",
        "watable": "latest",
        "flight": "latest",
        "jqcron": "https://github.com/arnapou/jqcron.git",
    }
)

css = Bundle(
    'vendors/bootstrap/dist/css/bootstrap.css',
    'vendors/bootstrap-datepicker-eyecon/css/datepicker.css',
    'vendors/watable/watable.css',
    'css/checker/admin/style.css',
    'vendors/jqcron/src/jqCron.css',
    filters=CleanCSSFilter(),
    output='checker.css',
    weight=30,
    bower={
        "bootstrap": "latest",
        "bootstrap-datepicker-eyecon": "latest",
        "watable": "latest",
    }
)
