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

# from invenio_checker.clients.worker import (
#     RedisWorker,
#     get_lock_partial,
# )
import pytest
from copy import deepcopy
from mock import call, create_autospec
from frozendict import frozendict as fd
from invenio_base.wrappers import lazy_import
from intbitset import intbitset as ibs

get_record = lazy_import('invenio_checker.conftest.conftest2.get_record')
_pytest_sessionfinish = lazy_import('invenio_checker.conftest.conftest2._pytest_sessionfinish')
pytest_sessionstart = lazy_import('invenio_checker.conftest.conftest2.pytest_sessionstart')
_get_fullpatches_of_last_run = lazy_import('invenio_checker.conftest.conftest2._get_fullpatches_of_last_run')


class TestConftest(object):

    def test_sessionfinish_sends_patches_to_redis(self, mocker, m_conn):

        # Given some expected patches
        patch1 = {'task_id': 'abc', 'recid': 1, 'patch': '[{"path": "/recid", "value": 10, "op": "replace"}]'}
        patch2 = {'task_id': 'abc', 'recid': 2, 'patch': '[{"path": "/recid", "value": 20, "op": "replace"}]'}
        m_get_fullpatches_of_last_run = mocker.Mock(return_value=(patch1, patch2))
        mocker.patch('invenio_checker.conftest.conftest2._get_fullpatches_of_last_run', m_get_fullpatches_of_last_run)

        # for eliotify
        mocker.patch('invenio_checker.conftest.conftest2.Session', mocker.MagicMock())

        # ..and some names the code expects
        worker = mocker.Mock(uuid='abc', conn=m_conn, patch_to_redis=mocker.Mock(), retry_after_ids={})
        invenio_eliot_action = mocker.Mock()
        invenio_reporters = []
        exitstatus = 0
        invenio_records = {'original': {}, 'modified': {}, 'temporary': {}}

        # Run sessionfinish on success
        _pytest_sessionfinish(invenio_eliot_action, worker, invenio_reporters, invenio_records, exitstatus)

        # Assert that two patches were sent to redis
        assert worker.patch_to_redis.call_count == 2
        worker.patch_to_redis.assert_has_calls([
            call({'task_id': 'abc', 'recid': 1, 'patch': '[{"path": "/recid", "value": 10, "op": "replace"}]'}),
            call({'task_id': 'abc', 'recid': 2, 'patch': '[{"path": "/recid", "value": 20, "op": "replace"}]'}),
        ], any_order=True)
        # Assert mocked get_fullpatches_of_last_run was called as expected
        m_get_fullpatches_of_last_run.assert_called_once_with('modified', invenio_records, worker)
        # Assert that the storage is cleared
        assert not invenio_records['original']
        assert not invenio_records['modified']
        assert not invenio_records['temporary']
        # Assert eliot action was closed
        invenio_eliot_action.finish.assert_called_once_with()

    def test_get_fullpatches_of_last_run(self, mocker):

        # Given some fullpatches
        records_o = {1: fd({'recid': 1}), 2: fd({'recid': 2}), 3: fd({'recid': 3})}
        records_m = {1: fd({'recid': 10}), 2: fd({'recid': 20}), 3: fd({'recid': 3})}

        # And some mocks
        invenio_records = {'modified': deepcopy(records_m), 'original': deepcopy(records_o)}
        worker = mocker.Mock(task_id='abc')

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
        mocker.patch('invenio_checker.conftest.conftest2.jsonpatch', m_jsonpatch)

        # Mock make_fullpatch
        from invenio_checker.clients.worker import make_fullpatch
        m_make_fullpatch = create_autospec(make_fullpatch)
        mocker.patch('invenio_checker.conftest.conftest2.make_fullpatch', m_make_fullpatch)

        # Run
        ret = tuple(_get_fullpatches_of_last_run('modified', invenio_records, worker))

        # Ensure that two patches were sent to redis
        assert len(ret) == 2
        assert set(ret) == {
            m_make_fullpatch(1, 'hash1', '[{"path": "/recid", "value": 10, "op": "replace"}]', 'abc'),
            m_make_fullpatch(2, 'hash1', '[{"path": "/recid", "value": 20, "op": "replace"}]', 'abc'),
        }

    def test_worker_clears_items_when_blocked(self, mocker, m_option):
        from invenio_checker.enums import StatusWorker

        mocker.patch('invenio_checker.conftest.conftest2._ensure_only_one_test_function_exists_in_check',
                     mocker.Mock())

        mocker.patch('invenio_checker.conftest.conftest2._get_restrictions_from_check_class',
                     mocker.Mock(return_value=('abc', 'def')))

        mocker.patch('invenio_checker.conftest.conftest2.load_reporters',
                     mocker.Mock())

        m_worker_conflicts_with_currently_running = \
            mocker.patch('invenio_checker.conftest.conftest2._worker_conflicts_with_currently_running',
                         mocker.Mock(
                             return_value=(
                                 mocker.Mock(uuid='ID1'),
                                 mocker.Mock(uuid='ID2'),
                             )))

        mocker.patch('invenio_checker.conftest.conftest2.Session',
                     mocker.MagicMock())

        class Item(object):
            def __init__(self, i):
                self.i = i
                self.fixturenames = ''

        m_items = [Item(1), Item(2), Item(3)]
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
                                 '_worker_conflicts_with_currently_running')
        # mock_manager end

        from invenio_checker.conftest.conftest2 import _pytest_collection_modifyitems
        _pytest_collection_modifyitems(m_task_arguments, m_worker, m_items, None, m_option,
                                       {1, 2}, {1, 2, 3, 4})

        assert m_worker.status == StatusWorker.ready
        assert m_items == []
        assert m_worker.retry_after_ids == {'ID1', 'ID2'}

        from ..conftest import contains_sparse_sublist
        assert contains_sparse_sublist(
            mock_manager.mock_calls,
            [
                call.lock.__enter__(),
                call._worker_conflicts_with_currently_running(m_worker),
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
        from invenio_checker.clients.worker import _workers_touch_common_paths
        assert _workers_touch_common_paths(paths1, paths2) == expected_result
        assert _workers_touch_common_paths(paths2, paths1) == expected_result


    @pytest.mark.parametrize("_,recids1,recids2,expected_result", [
        ("both same", ibs({1, 2, 3}), ibs({1, 2, 3}), True),
        ("subset", ibs({1, 2, 3}), ibs({1, 2}), True),
        ("different", ibs({1, 2}), ibs({2, 3}), True),
        ("one none", None, ibs({1, 2}), False),
        ("both none", None, None, False),
    ])
    def test_workers_touch_recids_resolves(self, _, recids1, recids2,
                                           expected_result):
        from invenio_checker.clients.worker import _workers_touch_common_records
        assert _workers_touch_common_records(recids1, recids2) == expected_result
        assert _workers_touch_common_records(recids2, recids1) == expected_result


# class TestFixtures(object):

#     def test_search_fixture_returns_from_get_record(self, mocker, m_request):
#         m_query = mocker.patch('invenio_checker.conftest.check_fixtures.Query', \
#         mocker.Mock(                                 # Query
#             return_value=mocker.Mock(                #      ()
#                 search=mocker.Mock(                  #        .search
#                     return_value=mocker.Mock(        #               ()
#                         recids=[1, 2, 3])))))        #                 .recids

#         m_request.session.config.option.redis_worker.get_record_orig_or_mem = \
#             mocker.Mock(
#                 side_effect=lambda recid: {'recid': recid})

#         from invenio_checker.conftest.check_fixtures import search
#         result = search(m_request)('foobar')
#         lresult = list(result.records)

#         recs = m_request.session.invenio_records
#         assert recs == \
#             {'temporary': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}},
#              'original': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}},
#              'modified': {1: {'recid': 1}, 2: {'recid': 2}, 3: {'recid': 3}}}

#         assert recs['original'] is not recs['modified']

#         m_query.assert_called_once_with('foobar')
#         assert lresult == [{'recid': 1}, {'recid': 2}, {'recid': 3}]

#     def test_get_record_sets_all_on_first_call(
#             self, mocker,
#             m_request,
#     ):
#         invenio_records = m_request.session.invenio_records

#         # Mock
#         rec = {'a': 1, 'recid': 1}
#         get_record_orig_or_mem = mocker.Mock(return_value=rec)
#         m_request.session.config.option.redis_worker.get_record_orig_or_mem = \
#             get_record_orig_or_mem

#         # Run
#         ret = get_record(m_request)(1)

#         # Assert
#         assert invenio_records['original'][1] is rec

#         assert invenio_records['modified'][1] is not rec
#         assert invenio_records['modified'][1] == rec

#         assert invenio_records['temporary'][1] is invenio_records['modified'][1]

#         get_record_orig_or_mem.asset_called_once_with(1)

#         assert ret is invenio_records['temporary'][1]

#     def test_get_record_returns_same_on_second_call(
#             self, mocker,
#             m_request,
#     ):
#         invenio_records = m_request.session.invenio_records

#         # Add a record
#         rec = {1: 'a'}
#         rec_copy = deepcopy(rec)

#         # Prepare store
#         invenio_records['original'] = rec
#         invenio_records['modified'] = rec_copy
#         invenio_records['temporary'] = rec

#         # Run
#         ret = get_record(m_request)(1)

#         # Assert
#         assert invenio_records['original'] is invenio_records['temporary']
#         assert invenio_records['modified'] is not invenio_records['original']

#         assert ret is m_request.session.invenio_records['temporary'][1]
