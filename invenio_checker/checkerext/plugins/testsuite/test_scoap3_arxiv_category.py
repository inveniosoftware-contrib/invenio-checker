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

"""Unit tests for scoap3_arxiv_category."""

import os

from mock import patch
from nose_parameterized import parameterized

from invenio.base.wrappers import lazy_import
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase
from common import mock_open


arxiv_category = lazy_import('invenio.modules.checker.checkerext.plugins.scoap3_arxiv_category')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    # def test_primary_report_number_is_correctly_changed(self):
    #     check_record = arxiv_category.check_record
    #     cases = (
    #         (Record({'primary_report_number': '12.34'}        , 'arXiv:12.34'),
    #         (Record({'primary_report_number': 'ab12.34 '}     , 'ab12.34 '),
    #         (Record({'primary_report_number': '012.55'}       , 'arXiv:012.55'),
    #         (Record({'primary_report_number': '12.341'}       , 'arXiv:12.341'),
    #         (Record({'primary_report_number': '12,34'})        , '12,34'),
    #         (Record({'primary_report_number': '12,.34'}       , '12,.34'),
    #         (Record({'primary_report_number': 'one 12.34 two'}, 'one 12.34 two'),
    #     )

    #     for record, expected_field in cases:
    #         result_record = check_record(record)
    #         self.assertEqual(result_record['primary_report_number'],
    #                          expected_field)

    @parameterized.expand((
        # test_name         , record                                            , arxiv_id         , expceted_exception
        ('new_style'        , {'primary_report_number': 'arXiv:1234.1234v2'}    , '1234.1234v2'    , None) ,
        ('old_style'        , {'primary_report_number': 'arXiv:hep-th/9901001'} , 'hep-th/9901001' , None) ,
        ('without_prefix'   , {'primary_report_number': 'hep-th/9901001'}       , 'hep-th/9901001' , None) ,
        ('non_arxiv_format' , {'primary_report_number': 'foobar'}               , None             , None) ,
        ('no_key'           , {}                                                , None             , None) ,
    ))
    def test_getting_arxiv_id_from_record(self, _, record, expected_arxiv_id, expected_exception):
        _get_arxiv_id_from_record = arxiv_category._get_arxiv_id_from_record
        if expected_exception:
            self.assertRaises(expected_exception,
                                _get_arxiv_id_from_record, record)
        else:
            self.assertEqual(_get_arxiv_id_from_record(record),
                            expected_arxiv_id)

    @parameterized.expand((
            # test_name             , doi                              , arxiv_id    , excpeted_exception
            ('no_arxiv_id_present'  , '10.1088/0954-3899/37/7A/075021' , None        , None      ) ,
            ('arxiv_id_is_returned' , '10.1088/0067-0049/192/2/18'     , '1001.4538' , None      ) ,
            ('inspire_net_down'     , '10.1088/0067-0049/192/2/18'     , '1001.4538' , IOError   ) ,
            ('no_record_in_reply'   , '10.1088/0067-0049/192/2/18'     , '1001.4538' , IndexError) ,
    ))
    def test_getting_arxiv_id_from_inspire(self, _, doi, arxiv_id, expected_exception):
        _get_arxiv_id_from_inspire = arxiv_category._get_arxiv_id_from_inspire
        with patch('invenio.modules.checker.checkerext.plugins.scoap3_arxiv_category.urllib.urlopen') as mock_urlopen:
            if expected_exception:
                mock_urlopen.side_effect = expected_exception
                self.assertRaises(expected_exception,
                                _get_arxiv_id_from_inspire, doi)
            else:
                mock_urlopen.return_value = mock_open(doi, 'r')
                self.assertEqual(_get_arxiv_id_from_inspire(doi),
                                arxiv_id)

    @parameterized.expand((
        # test_name          , new arxiv ID                     , record                              , expected record
        ('previous_existing' , '10.1103/PhysRevLett.112.241101' , {'primary_report_number': 'foobar'} , {'primary_report_number': 'arXiv:10.1103/PhysRevLett.112.241101'}) ,
        ('previous_None'     , '10.1103/PhysRevLett.112.241101' , {'primary_report_number': None}     , {'primary_report_number': 'arXiv:10.1103/PhysRevLett.112.241101'}) ,
        ('previous_missing'  , '10.1016/j.physletb.2012.08.020' , {}                                  , {'primary_report_number': 'arXiv:10.1016/j.physletb.2012.08.020'}) ,
    ))
    def test_adding_of_arxiv_to_record(self, _, new_arxiv_id, record, expected_record):
        _add_arxiv_to_record = arxiv_category._add_arxiv_to_record
        _add_arxiv_to_record(record, new_arxiv_id)
        self.assertEqual(record, expected_record)

    @parameterized.expand((
            # arxiv ID               , expected categories , expected_exception
            ('set_of_many'           , '1403.3985'         , set(('astro-ph.CO', 'gr-qc', 'hep-ph', 'hep-th'))  , None   ) ,
            ('set_of_one'            , '1207.7214'         , set(('hep-ex',))                                   , None   ) ,
            ('id_without_categories' , '1207.7214a'        , set()                                              , None   ) ,
            ('website_down'          , '1207.7214'         , set(('hep-ex',))                                   , IOError) ,
    ))
    def test_finding_arxiv_categories_from_arxiv_org(self, _, arxiv_id, expected_categories, expected_exception):
        _get_arxiv_categories_from_arxiv_org = arxiv_category._get_arxiv_categories_from_arxiv_org
        with patch('invenio.modules.checker.checkerext.plugins.scoap3_arxiv_category.urllib.urlopen') as mock_urlopen:
            if expected_exception:
                mock_urlopen.side_effect = expected_exception()
                self.assertRaises(expected_exception,
                                    _get_arxiv_categories_from_arxiv_org, arxiv_id)
            else:
                mock_urlopen.return_value = mock_open(arxiv_id, 'r')
                self.assertEqual(_get_arxiv_categories_from_arxiv_org(arxiv_id),
                                    expected_categories)


TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
