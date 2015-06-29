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

"""Unit tests for add_orcid."""

import os

from mock import patch, Mock
from nose_parameterized import parameterized
from xml.dom.minidom import parse

from common import mock_open
from invenio.base.wrappers import lazy_import
from invenio.testsuite import InvenioTestCase


add_orcid = lazy_import('invenio.modules.checker.checkerext.plugins.scoap3_add_orcid')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # test_name           , xml_basename               , expected_orcids , expceted_exception
        ('ce_author'          , 'orcid_ce:author'          , ["", "ORCID:0000-0001-6037-7975", ""]),
        ('contrib'            , 'orcid_contrib'            , ["ORCID:0000-0003-2078-3535", "ORCID:0000-0001-5604-2531", "ORCID:0000-0003-1281-7977", "ORCID:0000-0001-6322-1386", "ORCID:0000-0003-3606-0104"]),
        ('ce_author_no_orcid' , 'orcid_ce:author_no_orcid' , ["", "", ""]),
    ))
    def test_getting_of_orcids_from_xml_document(self, _, xml_basename, expected_orcids):
        _get_orcids = add_orcid._get_orcids
        with mock_open(xml_basename, 'r') as xml_file:  # TODO: Rename to xml_open
            xml_data = parse(xml_file)
            orcids = _get_orcids(xml_data)
            self.assertEquals(orcids, expected_orcids)

    @parameterized.expand((
        # test_name , author_dict, orcid , expected_author_dict
        ('no_orcid'            , {}                          , "ORCID:0000-0001-6037-7975", {'orcid': "ORCID:0000-0001-6037-7975"}),
        ('with_previous_orcid' , {'orcid': 'some_old_orcid'} , "ORCID:0000-0001-6037-7975", {'orcid': "ORCID:0000-0001-6037-7975"}),
    ))
    def test_adding_orcid_to_author(self, _, author_dict, orcid, expected_author_dict):
        _set_orcid = add_orcid._set_orcid
        new_author_dict = _set_orcid(author_dict, orcid)
        self.assertEquals(new_author_dict, expected_author_dict)

    # @parameterized.expand((
    #     # test_name , record, expected_record
    #     ('no_orcid' , {'recid': 1, 'authors': [{'first_name': '1'}, {'first_name': '2'}, {'first_name': '3'}]} ),
    # ))
    # def test_checking_of_record(self, _, record):
    #     check_record = add_orcid.check_record

TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
    #     check_record(record)
