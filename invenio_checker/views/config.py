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

"""
CheckerRule columns configuration.

This file contains the WATable configuration for the CheckerRule model.
This is a Python file, therefore the configuration consists of a dictionary,
where the keys are the column names of the model and their respective value
is a dictionary containing the options. The WATable jQuery library obtains
this information in JSON format, by making a POST request (see admin.py view).

You can find more details about the applicable options in the WATable examples
on the official project website located at: https://github.com/wootapa/watable

Some basic explanation of the available options.

- hidden: true | false
    Whether the column should not be visible.
- index: <integer>
    The order in which the column should be displayed.
- type: "string" | "number" | "bool" | "date"
    The column data type.
- friendly: <string>
    The column's display name.
- format: <HTML>
    Allows you to fine tune the column's display with HTML.
    The friendly name can be referred to with {0}.
- unique: true | false
    Whether the column values will be unique.
    This is required if you want checkable rows, databinding or to use the
    rowClicked callback. Be certain the values are really unique or weird things
    will happen.
- placeHolder: <string>
    The filter value indicator. Does not apply to columns of type bool.
    Recommended to override as it defaults to John Doe.
- filter: <string>
    Allows you to set the initial filter.
- sortOrder: "asc" | "desc"
    Sets the default sorting order.
- tooltip: <string>
    Some additional info about the column (shown on column name hover).
- filterTooltip: <string>
    Some additional info about the column filter (shown on column filter hover).
- sorting: true | false
    Enables or disables sorting.
"""

task_mapping = {
    "name": {
        "index": 1,
        "type": "string",
        "friendly": "Name",
        "format": "<a href='#' target='_blank'>{0}</a>",
        "unique": True,
    },
    "plugin": {
        "index": 2,
        "type": "string",
        "friendly": "Plugin",
    },
    "arguments": {
        "index": 3,
        "type": "string",
        "friendly": "Arguments",
    },
    "option_holdingpen": {
        "index": 4,
        "type": "bool",
        "friendly": "HoldingPen",
    },
    "option_consider_deleted_records": {
        "index": 5,
        "type": "bool",
        "friendly": "Consider deleted records",
    },
    "filter_pattern": {
        "index": 6,
        "type": "string",
        "friendly": "Filter pattern",
    },
    "filter_records": {
        "index": 7,
        "type": "string",
        "friendly": "Filter records",
    },
    "schedule": {
        "index": 8,
        "type": "string",
        "friendly": "Schedule",
    },
    "temporary": {
        "index": 9,
        "type": "bool",
        "friendly": "Temporary",
    },
}
# TODO: add last_run

check_mapping = {
    "name": {
        "index": 1,
        "type": "string",
        "friendly": "Name",
        "unique": True,
    },
    "description": {
        "index": 2,
        "type": "string",
        "friendly": "Description",
    },
}

log_mapping = {
    "task": {
        "index": 1,
        "type": "string",
        "friendly": "Task",
    },
    "start_date": {
        "index": 2,
        "type": "date",
        "friendly": "Start date",
        "sorting": True,
        "sortOrder": "desc",
    },
    "status_update_date": {
        "index": 3,
        "type": "date",
        "friendly": "Last status change",
    },
    "status": {
        "index": 4,
        "type": "string",
        "friendly": "Status",
    },
    "owner": {
        "index": 5,
        "type": "string",
        "friendly": "Owner",
    },
    "owner_id": {
        "index": 6,
        "hidden": True,
        "type": "number",
        "friendly": "Owner ID",
    },
    "uuid": {
        "index": 7,
        "friendly": "UUID",
        "type": "string",
        "format": '<div class="monospace">{0}</a>',
        "unique": True,
    }
}
