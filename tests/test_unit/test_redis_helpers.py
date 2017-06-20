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

import importlib

from mock import (
    call,
)
import pytest

from invenio_checker.clients.redis_helpers import (
    get_all_tasks_in_celery,
    _get_all_masters,
    _get_things_in_redis,
)

class TestRedisHelpers(object):

    def test_all_tasks_in_celery_returns_set_of_flat_tasks(self, mocker):

        m_insp = mocker.Mock()
        m_insp.active = mocker.Mock(return_value={'a': [1, 2]})
        m_insp.scheduled = mocker.Mock(return_value={'b': [3, 2]})

        m_inspect = mocker.patch('invenio_checker.clients.redis_helpers.inspect',
                                 return_value=m_insp)

        ret = get_all_tasks_in_celery()
        assert ret == {1, 2, 3}

    def test_all_task_in_celery_does_not_hang_when_celery_down(self, mocker):

        m_insp = mocker.Mock()
        m_insp.active = mocker.Mock(return_value=None)
        m_insp.scheduled = mocker.Mock(return_value=None)

        m_inspect = mocker.patch('invenio_checker.clients.redis_helpers.inspect',
                                 return_value=m_insp)

        ret = get_all_tasks_in_celery()
        assert ret == {}

    def test_get_all_masters_returns_all_masters_in_redis(self, mocker):

        m_conn = mocker.Mock()
        m_conn.scan_iter = mocker.Mock(
            return_value=(i for i in ('invenio_checker:master:uuid1:workers',
                                      'invenio_checker:master:uuid2:workers'))
        )

        m_RedisMaster = mocker.patch('invenio_checker.clients.master.RedisMaster')

        ret = _get_all_masters(m_conn)
        assert len(ret) == 2

        m_conn.scan_iter.assert_called_once_with('invenio_checker:master:*:workers')
        m_RedisMaster.assert_has_calls((call('uuid1'), call('uuid2')),
                                       any_order=True)
        assert m_RedisMaster.call_count == 2

    def test_get_things_in_redis_returns_all_the_things(self, mocker):

        prefixes = ('invenio_checker:master:{uuid}:foo',
                    'invenio_checker:master:{uuid}:bar')

        existing = ('invenio_checker:master:1:foo',
                    'invenio_checker:master:2:foo',
                    'invenio_checker:master:2:bar',
                    'invenio_checker:master:3:baz',)

        m_conn = mocker.Mock()
        m_conn.scan_iter = mocker.Mock(return_value=(i for i in existing))

        mocker.patch('invenio_checker.clients.redis_helpers.get_redis_conn',
                     mocker.Mock(return_value=m_conn))

        assert _get_things_in_redis(prefixes) == {'1', '2', '3'}

    @pytest.mark.parametrize("cls_import, fn_call, keys_str", [
        ('invenio_checker.clients.master.RedisMaster', 'get_masters_in_redis', 'keys_master'),
        ('invenio_checker.clients.worker.RedisWorker', 'get_workers_in_redis', 'keys_worker'),
    ])
    def test_get_x_in_redis_rets_x(self, cls_import, fn_call, keys_str, mocker):

        m_RedisClient = mocker.patch(
            cls_import,
            side_effect=lambda x: x,
        )
        m_get_things_in_redis = mocker.patch(
            'invenio_checker.clients.redis_helpers._get_things_in_redis',
            return_value={'1', '2', '3'}
        )
        helpers = importlib.import_module('invenio_checker.clients.redis_helpers')
        get_clients_in_redis = getattr(helpers, fn_call)

        from invenio_checker.clients.master import keys_master
        from invenio_checker.clients.worker import keys_worker
        keys = {'keys_master': keys_master,
                'keys_worker': keys_worker}

        assert len(get_clients_in_redis()) == 3
        m_get_things_in_redis.assert_called_once_with(keys[keys_str])
        m_RedisClient.assert_has_calls([call('1'), call('2'), call('3')],
                                       any_order=True)
        assert m_RedisClient.call_count == 3

    def test_set_identifier_sets_identifier(self, mocker):

        from invenio_checker.clients.redis_helpers import set_identifier

        class Test(object):

            @set_identifier('teal {key}')
            def return_identifier(self):
                return self._identifier  # pylint: disable=no-member

        def ret_key(string):
            return string.format(key='KEY')

        test = Test()
        test.fmt = mocker.Mock(side_effect=ret_key)
        assert test.return_identifier() == 'teal KEY'

    def test_SetterProperty(self):

        from invenio_checker.clients.redis_helpers import SetterProperty

        class Test(object):

            @SetterProperty
            def foo(self, new_val):
                self.bar = new_val + 1
                return new_val + 1

        test = Test()
        test.foo = 1
        assert test.bar == 2

    # def test_RedisClient_in_celery_returns_the_truth(self, mocker):
