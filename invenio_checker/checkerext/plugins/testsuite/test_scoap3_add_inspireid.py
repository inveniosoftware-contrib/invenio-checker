# -*- coding: utf-8 -*-
##
## This file is part of SCOAP3.
## Copyright (C) 2015 CERN.
##
## SCOAP3 is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## SCOAP3 is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with SCOAP3; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Correct missing arxiv category."""

from invenio.modules.jsonalchemy.reader import split_blob
from mock import patch, Mock

from nose_parameterized import parameterized, param

from invenio.base.wrappers import lazy_import
from invenio.testsuite import make_test_suite, run_test_suite, InvenioTestCase

from common import is_exception_of

from io import StringIO


def mock_urlopen(retval, exception=None, bad_response=False):
    urlopen = Mock()
    if bad_response:
        urlopen.return_value = StringIO(unicode([1, 2, 'hello']))
    elif exception:
        urlopen.side_effect = exception
    elif retval is None:
        urlopen.return_value = StringIO(unicode([]))
    else:
        urlopen.return_value = StringIO(unicode([retval]))
    return urlopen


def mock_db(retvals):
    from sqlalchemy.orm.exc import NoResultFound
    db = Mock()
    db.session.query = Mock()
    db.session.query.return_value = db.session.query
    # db.session.assert_called_with(..)  # TODO: Fill in when we have table

    db.session.query.filter = Mock()
    db.session.query.filter.return_value = db.session.query.filter
    # db.session.filterassert_called_with(..)  # TODO: Fill in when we have table

    db.session.query.filter.all.return_value = retvals

    try:
        db.session.query.filter.one.return_value = retvals[0]
    except IndexError:
        db.session.query.filter.one.side_effect = NoResultFound
    return db


class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # test_name,               , record                                                                           , expected_idx , expected_exception
        param('exists'             , {'system_control_number': [{'institute': 'INSPIRE'}]}                            , 0),
        param('exists_idx'         , {'system_control_number': [{'institute': 'OTHER'}, {'institute': 'INSPIRE'}]}    , 1),
        param('two_exist'          , {'system_control_number': [{'institute': 'INSPIRE'}, {'institute': 'INSPIRE'}]}  , expected_exception=KeyError),
        param('empty_institute'    , {'system_control_number': [{'institute': ''}]}                                   ),
        param('nul_control_number' , {'system_control_number': [{}]}                                                  ),
        param('no_control_number'  , {'system_control_number': []}                                                    ),
    ))
    def test_getting_of_system_control_number_struct_of_inspire(self, _, record, expected_idx=None, expected_exception=None):
        from invenio.modules.checker.checkerext.plugins.scoap3_add_inspire_id import get_inspire_id
        run_check = lambda: get_inspire_id(record)

        if expected_exception:
            with self.assertRaises(expected_exception):
                run_check()
            return
        if expected_idx == None:
            inspire_id_struct = run_check()
            self.assertEqual(inspire_id_struct, None)
        else:
            expected_result = record['system_control_number'][expected_idx]
            inspire_id_struct = run_check()
            self.assertIs(inspire_id_struct, expected_result)


    @parameterized.expand((
        #'name'          , doi                             , expected_recid
        ('doi'           , '10.1088/1475-7516/2014/10/069' , 4534),
        ('bad_doi'       , 'bad_doi'                       , None),
        ('bad_call'      , None                            , None),
    ))
    def test_getting_inspireid_from_database(self, _, doi, expected_recid):
        from invenio.modules.checker.checkerext.plugins.scoap3_add_inspire_id import get_inspireid_from_database
        with patch('invenio.ext.sqlalchemy.db', mock_db([expected_recid])) as mocked_db:
            if is_exception_of(expected_recid, Exception):  # TODO: Split expected_exception
                self.assertRaises(expected_recid,
                                  get_inspireid_from_database, doi)
            else:
                returned_recid = get_inspireid_from_database(doi)
                self.assertEquals(returned_recid, expected_recid)
                self.assertTrue(mocked_db.session.query.filter.called or mocked_db.session.filter.called)
                self.assertTrue(mocked_db.session.query.filter.all.called or mocked_db.session.query.filter.one.called)


    @parameterized.expand((
        #'name'         , doi & arxiv                              , expected_recid      , expected_url                                                    , expected_exception
        param('doi'          , {'doi': '10.1088/1475-7516/2014/10/069'} , 4534 , 'http://inspirehep.net/search?doi=10.1088%2F1475-7516%2F2014%2F10%2F069') ,
        param('arxiv'        , {'arxiv': 'arXiv:1404.3283'}             , 4024 , 'http://inspirehep.net/search?arxiv=arXiv%3A1404.3283')                   ,
        param('bad_doi'      , {'doi': 'bad_doi'}                       , None , 'http://inspirehep.net/search?doi=bad_doi')                               ,
        param('bad_arxiv'    , {'arxiv': 'bad_arxiv_id'}                , None , 'http://inspirehep.net/search?arxiv=bad_arxiv_id')                        ,
        param('bad_call'     , {}                                       , None , None                                                                      , TypeError       ) ,
        param('bad_response' , {'doi': '10.1088/1475-7516/2014/10/069'} , None , 'http://inspirehep.net/search?doi=10.1088%2F1475-7516%2F2014%2F10%2F069'  , TypeError, bad_response=True       ) ,
        param('network_fail' , {'doi': '10.1088/1475-7516/2014/10/069'} , None , 'http://inspirehep.net/search?doi=10.1088%2F1475-7516%2F2014%2F10%2F069'  , IOError         ) ,
    ))
    def test_getting_inspireid_from_inspire_net(self, _, kwargs, expected_recid, expected_url, expected_exception=None, bad_response=False):
        from invenio.modules.checker.checkerext.plugins.scoap3_add_inspire_id import get_inspireid_from_inspire_net

        with patch('urllib2.urlopen', mock_urlopen(expected_recid, expected_exception, bad_response)) as mocked_urlopen:
            # See if we got the correct `recid` or desired exception
            if expected_exception:
                self.assertRaises(expected_exception,
                                  get_inspireid_from_inspire_net, **kwargs)
            else:
                returned_recid = get_inspireid_from_inspire_net(**kwargs)
                self.assertEquals(returned_recid, expected_recid)

            # See if `urlopen` was called with the correct argument
            # FIXME http library
            try:
                used_args = mocked_urlopen.call_args[0]
            except TypeError:
                self.assertTrue(expected_url is None)
            else:
                expected_args = (expected_url ,)
                self.assertEquals(used_args, expected_args)

TEST_SUITE = make_test_suite(TestChanges)

if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
