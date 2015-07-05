# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2015 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Unit tests for arxiv_prefix."""

from nose_parameterized import parameterized, param

from invenio.base.wrappers import lazy_import
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase
from datetime import datetime

amend_publication_info = lazy_import('invenio.modules.checker.checkerext.checks.scoap3_amend_publication_info')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # record, expected_year
        ('no_creation_date',  {},                                      None),
        ('yes_creation_date', {'creation_date': datetime(1995, 1, 4)}, 1995),
    ))
    def test_resolution_of_year_from_creation_date(self, _, record, expected_year):
        _resolve_year_from_creation_date = amend_publication_info._resolve_year_from_creation_date
        resolved_year = _resolve_year_from_creation_date(record)
        self.assertEquals(resolved_year, expected_year)

    @parameterized.expand((
        # publication_info             , expected_publication_info
        ('', {'a': '', 'b':'-', 'c': 'foo'}, {'c': 'foo'}),
    ))
    def test_cleanup_of_publication_info(self, _, publication_info, expected_publication_info):
        _cleanup_publication_info = amend_publication_info._cleanup_publication_info
        resulting_publication_info = _cleanup_publication_info(publication_info)
        self.assertEquals(resulting_publication_info, expected_publication_info)


TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
