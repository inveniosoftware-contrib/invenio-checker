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

from invenio.ext.email import send_email
from invenio_checker.models import CheckerReporter, CheckerRule
from sqlalchemy.orm.exc import NoResultFound
from invenio.base.config import CFG_SITE_ADMIN_EMAIL
from enum import Enum

argument_schema = {
    'send_email': {'type': 'choice', 'values': ['always', 'never', 'on_failure'], 'label': 'Send email'}
}

class SendEmail(Enum):
    on_failure = 0
    always = 1
    never = 2


class MailReporter(object):
    def __init__(self, rule_name):
        try:
            self.reporter = CheckerReporter.query.filter_by(rule_name=rule_name).one()
        except NoResultFound:
            raise Exception("Missing or wrong reporter name!")
        task = CheckerRule.query.filter_by(rule_name=rule_name).one()
        self.rule_name = rule_name
        self.email = task.owner.email
        self.settings = self.reporter.arguments

    def report_exception(self, when, outrep_summary, location_tuple, formatted_exception=None, patches=None):
        send_email(CFG_SITE_ADMIN_EMAIL,
                   self.email,
                   "CHECKER EXCEPTION - rule: %s raised exception" % (self.reporter.rule_name,),
                   "%s\n %s\n %s\n %s" % (when, outrep_summary, location_tuple, formatted_exception))

    def report(self, user_readable_msg, location_tuple=None):
        send_email(CFG_SITE_ADMIN_EMAIL,
                   self.email,
                   "CHECKER LOG - rule: %s logging" % (self.reporter.rule_name,),
                   "%s\n %s" % (user_readable_msg, location_tuple))

    def finalize(self):
        pass


# TODO make reporter load correct settings from DB on initialization basing on name taken from task
def get_reporter(name):
    return MailReporter(name)
