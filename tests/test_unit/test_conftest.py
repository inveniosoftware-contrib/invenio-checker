# -*- coding: utf-8 -*-
#
# This file is part of Invenio Checker.
# Copyright (C) 2015 CERN.
#
# Invenio Checker is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio Checker is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio Checker; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

from invenio_checker.enums import StatusWorker
import redis
from invenio_checker.worker import (
    RedisWorker,
    get_lock_partial,
)
from pytest_mock import mock_module
import pytest
from copy import deepcopy
from mock import call
import mock

from invenio.base.wrappers import lazy_import
get_record = lazy_import('invenio_checker.conftest2.get_record')
pytest_sessionfinish = lazy_import('invenio_checker.conftest2.pytest_sessionfinish')
pytest_sessionstart = lazy_import('invenio_checker.conftest2.pytest_sessionstart')
get_fullpatches_of_last_run = lazy_import('invenio_checker.conftest2.get_fullpatches_of_last_run')

import mock
from functools import wraps


class TestConftest(object):
    def test_sessionfinish_sends_patches_to_redis(self,
                                                  mocker,
                                                  m_session):
#         def dummy_decorator(*whatever, **whatever_dude):
#             def outer(func):
#                 def inner(*args, **kwargs):
#                     return func(*args, **kwargs)
#                 return inner
#             return outer
#         mocker.patch('invenio_checker.conftest2.start_action_dec',
#                      dummy_decorator)

        patch1 = {'task_id': 'abc', 'recid': 1, 'patch': '[{"path": "/recid", "value": 10, "op": "replace"}]'}
        patch2 = {'task_id': 'abc', 'recid': 2, 'patch': '[{"path": "/recid", "value": 20, "op": "replace"}]'}
        m_get_fullpatches_of_last_run = mocker.Mock(return_value=(patch1, patch2))
        mocker.patch('invenio_checker.conftest2.get_fullpatches_of_last_run',
                     m_get_fullpatches_of_last_run)

        # Run
        pytest_sessionfinish(m_session, 0)

        # Ensure that two patches were sent to redis
        redis_worker = m_session.config.option.redis_worker
        assert redis_worker.patch_to_redis.call_count == 2
        redis_worker.patch_to_redis.assert_has_calls([
            call({'task_id': 'abc', 'recid': 1, 'patch': '[{"path": "/recid", "value": 10, "op": "replace"}]'}),
            call({'task_id': 'abc', 'recid': 2, 'patch': '[{"path": "/recid", "value": 20, "op": "replace"}]'}),
        ], any_order=True)
        m_get_fullpatches_of_last_run.assert_called_once_with('modified')

        # Assert that the storage so that it doesn't expand forever
        invenio_records = m_session.invenio_records
        assert not invenio_records['original']
        assert not invenio_records['modified']
        assert not invenio_records['temporary']

    def test_get_fullpatches_of_last_run(self, mocker, m_session):

        invenio_records = m_session.invenio_records
        records_o = {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}}
        records_m = {1: {'recid': 10}, 2: {'recid': 20}, 3: {'recid': 3}}
        invenio_records['modified'] = deepcopy(records_m)
        invenio_records['original'] = deepcopy(records_o)

        def jsonpatch(r1, r2):
            """Return objects that act like jsonpatches when bool is called."""
            p = mocker.MagicMock()
            if r1 == records_o[1] and r2 == records_m[1]:
                p.tostring.return_value = '[{"path": "/recid", "value": 10, "op": "replace"}]'
            elif r1 == records_o[2] and r2 == records_m[2]:
                p.tostring.return_value = '[{"path": "/recid", "value": 20, "op": "replace"}]'
            elif r1 == records_o[3] and r2 == records_m[3]:
                try:
                    p.__bool__.return_value = False
                except AttributeError:
                    p.__nonzero__.return_value = False
                p.tostring.return_value = '[]'
            else:
                raise Exception('Unwated patch attempted')
            return p

        # Mock jsonpatch  --  Returns jsonpatch.JsonPatch
        m_jsonpatch = mocker.Mock()
        m_jsonpatch.make_patch = jsonpatch
        mocker.patch('invenio_checker.conftest2.jsonpatch', m_jsonpatch)

        # Mock make_fullpatch
        from invenio_checker.worker import make_fullpatch
        m_make_fullpatch = mock.create_autospec(make_fullpatch)
        mocker.patch('invenio_checker.conftest2.make_fullpatch', m_make_fullpatch)

        # Run
        ret = tuple(get_fullpatches_of_last_run('modified'))

        # Ensure that two patches were sent to redis
        assert len(ret) == 2
        assert set(ret) == {
            m_make_fullpatch(1, 'hash1', '[{"path": "/recid", "value": 10, "op": "replace"}]', 'abc'),
            m_make_fullpatch(2, 'hash1', '[{"path": "/recid", "value": 20, "op": "replace"}]', 'abc'),
        }

    #def test_make_fullpatch(self):

    @pytest.mark.skipif(True, reason="not sure if worth it")
    def test_pytest_sessionstart_initializes(self, mocker, m_config):
        session = type('session', (object,), {})()
        session.config = m_config

        Session = type('Session', (object,), {})()
        mocker.patch('invenio_checker.conftest2.Session', Session)

        pytest_sessionstart(session)
        assert session.invenio_records == {  # pylint: disable=no-member
            'original': {}, 'modified': {}, 'temporary': {}}
        assert Session.session is session  # pylint: disable=no-member

    # TODO: Split into multiple
    def test_worker_clears_items_when_blocked(self, mocker, m_session):
        mocker.patch('invenio_checker.conftest2.ensure_only_one_test_function_exists_in_check',
                     mocker.Mock())

        mocker.patch('invenio_checker.conftest2.get_restrictions_from_check_class',
                     mocker.Mock(return_value=('abc', 'def')))

        m_worker_conflicts_with_currently_running = \
            mocker.patch('invenio_checker.conftest2.worker_conflicts_with_currently_running',
                         mocker.Mock(
                             return_value=(
                                 mocker.Mock(uuid='ID1'),
                                 mocker.Mock(uuid='ID2'),
                             )))

        m_items = [1, 2, 3]
        m_worker = mocker.Mock()
        m_worker.attach_mock(mocker.Mock(), 'retry_after_ids')

        m_worker.lock = mocker.MagicMock(
            return_value=mocker.MagicMock(spec=file))

        m_task_arguments = mocker.Mock()

        # This manager is used so that we can assert the order at which its
        # attached mocks were called.
        mock_manager = mocker.Mock()
        mock_manager.attach_mock(m_worker.lock(), 'lock')
        mock_manager.attach_mock(m_worker, 'worker')
        mock_manager.attach_mock(m_worker_conflicts_with_currently_running,
                                 'worker_conflicts_with_currently_running')
        # mock_manager end

        from invenio_checker.conftest2 import _pytest_collection_modifyitems
        _pytest_collection_modifyitems(m_session, m_task_arguments, m_worker,
                                       m_items)

        assert m_worker.status == StatusWorker.ready
        assert m_items == []
        assert m_worker.retry_after_ids == {'ID1', 'ID2'}

        # FIXME: sparse sublist
        assert contains_sparse_sublist(
            mock_manager.mock_calls,
            [
                call.lock.__enter__(),
                call.worker_conflicts_with_currently_running(m_worker),
                call.lock.__exit__(None, None, None),
            ]
        )

    @pytest.mark.parametrize("_,paths1,paths2,expected_result", [
        ("both none", None, None, True),
        ("one none", {"/a/b"}, None, True),
        ("two none", None, {"/a/b"}, True),
        ("both same", {"/a/b"}, {"/a/b"}, True),
        ("subpath", {"/a/b"}, {"/a"}, True),
        ("different", {"/a/b"}, {"/b"}, False),
        ("trailing slash", {"/a/b"}, {"/a/b/"}, True),
        ("one common", {"/a/b", "/a"}, {"/a"}, True),
    ])
    def test_workers_touch_common_paths_resolves(self, _, paths1, paths2,
                                                 expected_result):
        from invenio_checker.worker import _workers_touch_common_paths
        assert _workers_touch_common_paths(paths1, paths2) == expected_result
        assert _workers_touch_common_paths(paths2, paths1) == expected_result

class TestFixtures(object):

    def test_search_fixture_returns_from_get_record(self, mocker, m_request):
        m_query = mocker.patch('invenio_checker.conftest2.Query', \
        mocker.Mock(                                 # Query
            return_value=mocker.Mock(                #      ()
                search=mocker.Mock(                  #        .search
                    return_value=mocker.Mock(        #               ()
                        recids=[1, 2, 3])))))        #                 .recids

        m_request.session.config.option.redis_worker.get_record_orig_or_mem = \
            mocker.Mock(
                side_effect=lambda recid: {'recid': recid})

        from invenio_checker.conftest2 import search
        result = search(m_request)('foobar')
        lresult = list(result.records)

        recs = m_request.session.invenio_records
        assert recs == \
            {'temporary': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}},
             'original': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}},
             'modified': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}}}

        assert recs['original'] is not recs['modified']

        m_query.assert_called_once_with('foobar')
        assert lresult == [{'recid': 1}, {'recid': 2}, {'recid': 3}]

    def test_record_fixture_uses_get_record_on_param(self, mocker, m_request):

        m_query = mocker.patch('invenio_checker.conftest2.Query', \
        mocker.Mock(                                 # Query
            return_value=mocker.Mock(                #      ()
                search=mocker.Mock(                  #        .search
                    return_value=mocker.Mock(        #               ()
                        recids=[1, 2, 3])))))        #                 .recids

        m_request.session.config.option.redis_worker.get_record_orig_or_mem = \
            mocker.Mock(
                side_effect=lambda recid: {'recid': recid})

        m_request.param = 39

        from invenio_checker.conftest2 import record
        result = record(m_request)

        recs = m_request.session.invenio_records
        assert recs == \
            {'temporary': {39: {'recid': 39}},
             'original': {39: {'recid': 39}},
             'modified': {39: {'recid': 39}}}

        assert recs['original'] is not recs['modified']
        assert result == {'recid': 39}

    def test_get_record_sets_all_on_first_call(
            self, mocker,
            m_request,
    ):
        invenio_records = m_request.session.invenio_records

        # Mock
        rec = {'a': 1, 'recid': 1}
        get_record_orig_or_mem = mocker.Mock(return_value=rec)
        m_request.session.config.option.redis_worker.get_record_orig_or_mem = \
            get_record_orig_or_mem

        # Run
        ret = get_record(m_request)(1)

        # Assert
        assert invenio_records['original'][1] is rec

        assert invenio_records['modified'][1] is not rec
        assert invenio_records['modified'][1] == rec

        assert invenio_records['temporary'][1] is invenio_records['modified'][1]

        get_record_orig_or_mem.asset_called_once_with(1)

        assert ret is invenio_records['temporary'][1]

    def test_get_record_returns_same_on_second_call(
            self, mocker,
            m_request,
    ):
        invenio_records = m_request.session.invenio_records

        # Add a record
        rec = {1: 'a'}
        rec_copy = deepcopy(rec)

        # Prepare store
        invenio_records['original'] = rec
        invenio_records['modified'] = rec_copy
        invenio_records['temporary'] = rec

        # Run
        ret = get_record(m_request)(1)

        # Assert
        assert invenio_records['original'] is invenio_records['temporary']
        assert invenio_records['modified'] is not invenio_records['original']

        assert ret is m_request.session.invenio_records['temporary'][1]


def contains_sublist(lst, sublst):
    n = len(sublst)
    return any((sublst == lst[i:i+n]) for i in xrange(len(lst)-n+1))

def contains_sparse_sublist(lst, sublst):
    for i in range(len(sublst)-1):
        e1 = sublst[i]
        e2 = sublst[i+1]
        try:
            if lst.index(e1) > lst.index(e2):
                return False
        except IndexError:
            return False
    return True
