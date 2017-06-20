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

import os


class LocationTuple(object):
    """Common structure to identify the location of some code.

    This is useful for sending the same kind of tuple to the reporters, no
    matter what it is that we are reporting.

    The format is: (absolute_path, line number, domain)
    """

    @staticmethod
    def from_report_location(report_location):
        """Convert a `report_location` to a `LocationTuple`.

        :type report_location: tuple
        """
        fspath, lineno, domain = report_location
        return os.path.abspath(str(fspath)), lineno, domain

