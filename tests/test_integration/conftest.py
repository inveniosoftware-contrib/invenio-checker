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

import pytest


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(session, config, items):
    """
    invenio.testsuite.InvenioTestCase doesn't have an effect with py.test's
    setup/teardown functionality. So what we do to do cleanups is reorder the
    tests so that the cleanup test runs first.
    """

#     cleanup = None
#     for idx, item in enumerate(items):
#         if item.name == 'test_cleanup':
#             cleanup = item
#             items.pop(idx)
#             break

    def sort_by_id(item):
        name = item.name
        try:
            return int(name.split('_')[1], 8)
        except ValueError:
            return hash(item.name)

    items.sort(key=sort_by_id)

    # if cleanup:
    #     items.insert(0, cleanup)
    #     items.append(cleanup)

    # pytest.set_trace()

def pytest_addoption(parser):
    parser.addoption("--invenio-db-username", action="store", metavar="invenio_username",
        help="username to use when using the invenio database", default="root")
    parser.addoption("--invenio-db-password", action="store", metavar="invenio_password",
        help="password to use when using the invenio database", default="")
