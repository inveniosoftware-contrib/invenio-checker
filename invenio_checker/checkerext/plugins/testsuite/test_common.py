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
from invenio.base.wrappers import lazy_import
import random
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase


common = lazy_import('invenio.modules.checker.checkerext.plugins.common')

bibdoc_ids = {
    1: (11, 12, 13),
    2: (21, ),
    3: (),
}


def mock_glob(doc_id):
    directory_contents = {
        1: ['/opt/invenio/var/data/files/g5/1/a:5.xml',
            '/opt/invenio/var/data/files/g1/1/bc;32;sth.xml',
            '/opt/invenio/var/data/files/g1/1/kl;3.xml',
            '/opt/invenio/var/data/files/g1/1/foo bar;44.xml'],

        2: ['/opt/invenio/var/data/files/g1/2/filename;2;bar.xml'],

        3: ['/opt/invenio/var/data/files/g1/3/zb;hello.xml'],

        4: []
    }
    return Mock(return_value=directory_contents[doc_id])


def _mock_BibDoc(id_):
    bds = Mock()
    bds.get_id.return_value = id_
    return bds


def mock_BibRecDocs(recid):
    brd = Mock()
    brd.list_bibdocs.return_value = [_mock_BibDoc(id_) for id_ in bibdoc_ids[recid]]
    return brd


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # test_name , rec_id , expected_doc_ids , expceted_exception
        ('multiple' , 1      , bibdoc_ids[1]    )              ,
        ('one'      , 2      , bibdoc_ids[2]    )              ,
        ('none'     , 3      , bibdoc_ids[3]    )              ,
    ))
    def test_getting_of_doc_ids(self, _, recid, expected_doc_ids, expected_exception=None):
        with patch('invenio.legacy.bibdocfile.api.BibRecDocs', mock_BibRecDocs):
            get_doc_ids = common.get_doc_ids
            if expected_exception:
                self.assertRaises(expected_exception,
                                  get_doc_ids, recid)
            else:
                self.assertItemsEqual(get_doc_ids(recid),
                                      expected_doc_ids)

    @parameterized.expand((
        # test_name , doc_id, expected_file , expceted_exception
        ('multiple' , 1 , '/opt/invenio/var/data/files/g1/1/bc;32;sth.xml'     ),
        ('one'      , 2 , '/opt/invenio/var/data/files/g1/2/filename;2;bar.xml'),
        ('fail'     , 3 , None            ),
        ('none'     , 4 , None            ),
    ))
    def test_getting_of_latest_file(self, _, doc_id, expected_file):
        with patch('glob.glob', mock_glob(doc_id)) as mocked_glob:
            get_latest_file = common.get_latest_file
            self.assertEquals(expected_file, get_latest_file(doc_id))
            # self.assertEquals(mocked_glob.call_args[0],
            #                   (os.path.join("/opt/invenio/var/data/files/g0/", str(doc_id)), 'xml'))


TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
