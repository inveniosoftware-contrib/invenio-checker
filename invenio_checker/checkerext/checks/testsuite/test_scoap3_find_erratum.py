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

find_erratum = lazy_import('invenio.modules.checker.checkerext.checks.scoap3_find_erratum')


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        ('space_sep' , 'this is erratum for'    , {'ERRATUM'})     ,
        ('nothing'   , 'this is nothing'        , set())           ,
        ('colon_sep' , 'corrigendum: something' , {'CORRIGENDUM'}) ,
        ('two'       , 'corrigendum: erratum'   , {'CORRIGENDUM'   , 'ERRATUM'}) ,
    ))
    def test_finding_of_collections_in_title(self, _, title, expected_collections):
        _collections_in_title = find_erratum._collections_in_title
        collections_found = _collections_in_title(title)
        self.assertEquals(collections_found, expected_collections)


    @parameterized.expand((
        # test_name             , record                                   , expected_record
        ('exists_erratum'       , {'collections': {'additional': 'ERRATUM'}} , {'ERRATUM'}                , {'collections': {'additional': 'ERRATUM'}})     ,
        ('no_additional'        , {'collections': {'primary': 'coll'}}       , set()                      , {'collections': {'primary': 'coll'}})           ,
        ('no_collections'       , {}                                         , {'CORRIGENDUM'}            , {'collections': {'additional': 'CORRIGENDUM'}}) ,
        param('two_collections' , {}                                         , {'CORRIGENDUM', 'ERRATUM'} , expected_exception=KeyError)                    ,
    ))
    def test_tetriary_collection_is_correctly_set(self, _, record, found_collections, expected_record=None, expected_exception=None):
        _set_collections_in_record = find_erratum._set_collections_in_record
        run_check = lambda: _set_collections_in_record(record, found_collections)

        if expected_exception:
            with self.assertRaises(expected_exception):
                run_check()
        else:
            returned_record = run_check()
            self.assertEqual(returned_record, expected_record)


TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
