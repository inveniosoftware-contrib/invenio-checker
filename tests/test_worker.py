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
from intbitset import intbitset  # pylint: disable=no-name-in-module
from pytest_mock import mock_module
import pytest
import mock

# from .conftest import get_TestRedisWorker

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
            'record_hash': '123',
            'patch': {'a': 'b'},
        }

        from invenio_checker.worker import id_to_fullpatch
        assert id_to_fullpatch('invenio_checker:patches:ab:10:123',
                               {'a': 'b'}) == \
            expected_fullpatch

    def test_get_sorted_fullpatches_gets_sorted_patches(self, mocker):

        mocker.PropertyMock = mock_module.PropertyMock

        fullpatch_storage = {
            'invenio_checker:patches:ab:10:123': [{'a': 'b'}, {'c': 'd'}],
            'invenio_checker:patches:ab:11:123': [{'N': 'O'}],
            'invenio_checker:patches:cd:10:123': [{'g': 'h'}],
        }

        m_conn = mocker.Mock()

        m_conn.lrange.side_effect = lambda ident, start, stop: \
            fullpatch_storage[ident]

        def lpop():
            for patch in fullpatch_storage['invenio_checker:patches:ab:10:123']:
                yield patch
            yield None
        import redis; c = redis.StrictRedis()
        foo = lpop()
        m_conn.lpop = mock.create_autospec(c.lpop, side_effect=lambda x: next(foo))
        # TODO: Assert that x is the right thing

        m_conn.scan_iter.return_value = (
            i for i in fullpatch_storage.iterkeys()
            if i == 'invenio_checker:patches:ab:10:123'
        )

        m_conn.ttl.side_effect = lambda x: \
            4 if x == 'invenio_checker:patches:ab:10:123' else 5

        m_get_conn = mocker.patch('invenio_checker.worker.get_redis_conn',
                                  return_value=m_conn)

        # Run
        from invenio_checker.worker import get_sorted_fullpatches
        fullpatches = tuple(get_sorted_fullpatches(10, 'ab'))
        assert fullpatches == (
            {'recid': 10, 'record_hash': '123',
             'task_id': 'ab', 'patch': {'a': 'b'}},

            {'recid': 10, 'record_hash': '123',
             'task_id': 'ab', 'patch': {'c': 'd'}},
        )

        m_conn.scan_iter.assert_called_once_with('invenio_checker:patches:ab:10:*')
        # m_conn.lrange.assert_called_once_with('invenio_checker:patches:ab:10:123', 0, -1)

    @pytest.mark.parametrize("input_,intbitset_str", [
        ({1, 2, 3}, r'x\x9ccc@\x05\x00\x00p\x00\x07'),
        (None, TypeError()),
    ])
    def test_can_set_recids(self, input_, intbitset_str,
                            f_intbitset, m_conn, mocker,
                            get_TestRedisWorker):

        # Mock
        m_intbitset, m_intbitset_inst = \
            f_intbitset('invenio_checker.worker.intbitset',
                        on_fastdump=intbitset_str)
        worker = get_TestRedisWorker('abc', m_conn)

        # Run
        worker.allowed_recids = input_

        # Verify
        identifier = 'invenio_checker:worker:abc:allowed_recids'
        if input_ is None:
            m_conn.set.assert_called_once_with(identifier, None)
        else:
            m_conn.set.assert_called_once_with(identifier, intbitset_str)
        if not isinstance(intbitset_str, Exception):
            m_intbitset.assert_called_once_with(input_)
            m_intbitset_inst.fastdump.assert_called_once_with()

    @pytest.mark.parametrize("set_,intbitset_str", [
        ({1, 2, 3}, r'x\x9ccc@\x05\x00\x00p\x00\x07'),
        (None, TypeError()),
    ])
    def test_can_get_recids(self, intbitset_str, set_,
                            f_intbitset, m_conn,
                            get_TestRedisWorker):

        # Mock
        m_intbitset, m_intbitset_inst = \
            f_intbitset('invenio_checker.worker.intbitset')
        worker = get_TestRedisWorker('abc', m_conn)
        m_conn.get.return_value = intbitset_str

        # Run
        retval = worker.allowed_recids

        identifier = 'invenio_checker:worker:abc:allowed_recids'
        m_conn.get.assert_called_once_with(identifier)
        if not set_ is None:
            m_intbitset.assert_called_once_with(intbitset_str)

        assert retval == m_intbitset_inst

    #def test_worker_notifies_master_about_status_update(self): #TODO

    # Ensure worker inherits from everything import that we test TODO
