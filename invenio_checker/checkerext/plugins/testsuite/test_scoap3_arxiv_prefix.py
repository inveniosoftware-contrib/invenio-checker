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

"""Unit tests for arxiv_prefix."""

from nose_parameterized import parameterized

from invenio.base.wrappers import lazy_import
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase


arxiv_prefix = lazy_import('invenio.modules.checker.checkerext.plugins.scoap3_arxiv_prefix')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # test_name        , record                                           , expected_arxiv_id
        ('new_style_exists', {'primary_report_number': 'arXiv:1234.1234v2'}   , 'arXiv:1234.1234v2'   ),
        ('old_style_exists', {'primary_report_number': 'arXiv:hep-th/9901001'}, 'arXiv:hep-th/9901001'),
        ('new_style_broken', {'primary_report_number': '1234.1234v2'}         , 'arXiv:1234.1234v2'   ),
        ('bad_format'      , {'primary_report_number': 'hello there'}         , 'hello there'         ),
        ('missing'         , {}                                                                       ),
    ))
    def test_arxiv_prefix_is_correctly_amended(self, _, record, expected_arxiv_id=None):
        check_record = arxiv_prefix.check_record
        new_record = check_record(record)
        if expected_arxiv_id is not None:
            self.assertEqual(new_record['primary_report_number'],
                            expected_arxiv_id)
        else:
            self.assertFalse('primary_report_number' in new_record)

TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
