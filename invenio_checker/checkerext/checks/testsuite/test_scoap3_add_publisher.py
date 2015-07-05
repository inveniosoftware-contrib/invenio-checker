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
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase


add_publisher = lazy_import('invenio.modules.checker.checkerext.checks.scoap3_add_publisher')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        ('known_publisher'   , 'Physics Letters B' , 'Elsevier') ,
        ('unknown_publisher' , 'HURR DURR'         , None    )   ,
        ('emptystring'       , ''                  , None    )   ,
    ))
    def test_resolving_of_publisher(self, _, journal, expected_publisher):
        _resolve_publisher = add_publisher._resolve_publisher
        publisher = _resolve_publisher(journal)
        self.assertEquals(publisher, expected_publisher)

    @parameterized.expand((
        # test_name,      record,                                     publisher,  expected_record
        ('has_publisher', {'publication_info': {'publisher': 'abc'}}, 'Elsevier', {'publication_info': {'publisher': 'Elsevier'}}),
        ('no_publisher',  {'publication_info': {}                  }, 'Elsevier', {'publication_info': {'publisher': 'Elsevier'}}),
        ('no_pub_info',   {}                                        , 'Elsevier', {'publication_info': {'publisher': 'Elsevier'}}),
    ))
    def test_adding_of_publisher_to_record(self, _, record, publisher, expected_record):
        _set_publisher = add_publisher._set_publisher
        self.assertEqual(_set_publisher(publisher, record),
                         expected_record)

TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
