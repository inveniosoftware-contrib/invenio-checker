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

from invenio_workflows.api import start_delayed
from sqlalchemy.orm.exc import NoResultFound


class WorkflowReporter(object):
    def __init__(self, db_entry, execution):
        self.reporter = db_entry
        self.execution = execution

    def report_exception(self, when, outrep_summary, location_tuple, formatted_exception=None, patches=None):
        error_data = {'rule_name': self.reporter.rule_name,
                      'when': when,
                      'outrep_summary': outrep_summary,
                      'location_tuple': location_tuple,
                      'formatted_exception': formatted_exception,
                      'patches': patches}
        start_delayed('simple_reporting_workflow', [error_data], module_name="checker")

    def report(self, user_readable_msg, location_tuple=None):
        pass

    def finalize(self):
        pass

def get_reporter(db_entry, execution):
    return WorkflowReporter(db_entry, execution)
