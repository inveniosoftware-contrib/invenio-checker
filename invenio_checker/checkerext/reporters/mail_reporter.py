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

from invenio_ext.email import send_email
from invenio_base.config import CFG_SITE_ADMIN_EMAIL
from enum import Enum

argument_schema = {
    'send_email': {'type': 'choice', 'values': ['always', 'never', 'on_failure'], 'label': 'Send email'}
}


class SendEmail(Enum):
    on_failure = 0
    always = 1
    never = 2


class MailReporter(object):

    # FIXME: This should not send one e-mail per issue.

    def __init__(self, db_entry, execution):
        self.reporter = db_entry
        self.rule_name = db_entry.rule.name
        self.email = execution.owner.email
        self.send_email = self.reporter.arguments['send_email']

    def report_exception(self, when, outrep_summary, location_tuple,
                         formatted_exception=None, patches=None):
        if self.send_email in (SendEmail.always, SendEmail.on_failure):
            send_email(
                CFG_SITE_ADMIN_EMAIL, self.email,
                "CHECKER EXCEPTION - rule: %s raised exception" %
                (self.reporter.rule_name,),
                "%s\n %s\n %s\n %s\n %s" % (when, outrep_summary,
                                            location_tuple,
                                            formatted_exception, patches)
            )

    def report(self, user_readable_msg, location_tuple=None):
        if self.send_email in (SendEmail.always,):
            send_email(
                CFG_SITE_ADMIN_EMAIL, self.email,
                "CHECKER LOG - rule: %s logging" % (self.reporter.rule_name,),
                "%s\n %s" % (user_readable_msg, location_tuple)
            )

    def finalize(self):
        pass


def get_reporter(db_entry, execution):
    return MailReporter(db_entry, execution)
