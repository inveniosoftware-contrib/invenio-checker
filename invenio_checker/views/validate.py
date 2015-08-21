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

from invenio_checker.models import CheckerRule
from invenio_checker.views.config import checker_rule_mapping


def validate_checker_rule_mapping():
    table_cols = set([col.name for col in CheckerRule.__table__.columns])
    config_cols = set(checker_rule_mapping.keys())

    sym_diff = table_cols ^ config_cols
    if len(sym_diff) == 0:
        return "OK"
    else:
        return "ERROR"
