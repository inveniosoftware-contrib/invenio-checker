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


class TestWorker(object):
    def test_workers_with_unproc_results(self, mocker):
        w_c_run = mocker.Mock()
        w_c_run.in_celery = True
        w_c_run.status = StatusWorker.running

        w_c_ran = mocker.Mock()
        w_c_ran.in_celery = True
        w_c_ran.status = StatusWorker.ran

        w_c_failed = mocker.Mock()
        w_c_failed.in_celery = True
        w_c_failed.status = StatusWorker.failed

        w_C_ran = mocker.Mock()
        w_C_ran.in_celery = False
        w_C_ran.status = StatusWorker.ran

        mocker.patch('invenio_checker.worker.get_workers_in_redis',
                     mocker.Mock(return_value={w_c_run, w_c_ran, w_c_failed}))

        from invenio_checker.worker import get_workers_with_unprocessed_results
        assert get_workers_with_unprocessed_results() == {w_c_run, w_c_ran}

class TestFullpatch(object):
    def test_id_to_fullpatch_extracts_the_right_patch(self):
        # also tests `make_fullpatch`
        expected_fullpatch = {
            'task_id': 'ab',  # uuid
            'recid': 10,
            'record_hash': 123,
            'patch': {'a': 'b'},
        }

        from invenio_checker.worker import id_to_fullpatch
        assert id_to_fullpatch('invenio_checker:patches:ab:10:123',
                               {'a': 'b'}) == \
            expected_fullpatch

    def test_get_fullpatches_gets_sorted_patches(self, mocker):

        from pytest_mock import mock_module
        mocker.PropertyMock = mock_module.PropertyMock

        m_RedisWorker1 = mocker.Mock()
        type(m_RedisWorker1).a_used_dependency_of_ours_has_failed = \
            mocker.PropertyMock(return_value=False)

        m_RedisWorker2 = mocker.Mock()
        type(m_RedisWorker2).a_used_dependency_of_ours_has_failed = \
            mocker.PropertyMock(return_value=False)

        m_RedisWorker3 = mocker.Mock()
        type(m_RedisWorker3).a_used_dependency_of_ours_has_failed = \
            mocker.PropertyMock(return_value=True)

        m_RedisWorkers = {'ab': m_RedisWorker1,
                          'cd': m_RedisWorker2,
                          'ef': m_RedisWorker3}

        mocker.patch('invenio_checker.worker.RedisWorker',
                     side_effect=lambda x: m_RedisWorkers[x])

        m_conn = mocker.Mock()
        m_conn.scan_iter.return_value = \
            (i for i in ['invenio_checker:patches:ab:10:123',
                         'invenio_checker:patches:cd:10:123',
                         'invenio_checker:patches:ef:10:123'])
        fullpatch_storage = {
            'invenio_checker:patches:ab:10:123': [{'a': 'b'}, {'c': 'd'}],
            'invenio_checker:patches:cd:10:123': [{'e': 'f'}],
            'invenio_checker:patches:ef:10:123': [{'g': 'h'}],
        }
        m_conn.lrange.side_effect = lambda ident, start, stop: \
            fullpatch_storage[ident]
        m_conn.ttl.side_effect = lambda x: \
            4 if x == 'invenio_checker:patches:ab:10:123' else 5

        m_get_conn = mocker.patch('invenio_checker.worker.get_redis_conn',
                                  return_value=m_conn)

        from invenio_checker.worker import get_fullpatches
        fullpatches = get_fullpatches(10)
        assert fullpatches == [{'recid': 10, 'record_hash': 123,
                                'task_id': 'ab', 'patch': {'a': 'b'}},
                               {'recid': 10, 'record_hash': 123,
                                'task_id': 'ab', 'patch': {'c': 'd'}},
                               {'recid': 10, 'record_hash': 123,
                                'task_id': 'cd', 'patch': {'e': 'f'}}]

        m_conn.scan_iter.assert_called_once_with('invenio_checker:patches:*:10:*')
        # m_conn.lrange.assert_called_once_with(' # should we?

    #def test_worker_notifies_master_about_status_update(self):
