# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.


"""Pytest configuration."""

from __future__ import absolute_import, print_function

import pytest

from _pytest.python import SubRequest
from _pytest.main import Session


@pytest.fixture(scope="function")
def f_intbitset(mocker):
    def f_intbitset_(path_to_patch, on_fastdump=None):
        from intbitset import intbitset  # pylint: disable=no-name-in-module

        # Instance
        m_intbitset_inst = mocker.Mock(intbitset())
        if on_fastdump is not None:
            if isinstance(on_fastdump, Exception):
                m_intbitset_inst.fastdump.side_effect = on_fastdump
            else:
                m_intbitset_inst.fastdump.return_value = on_fastdump

        # Class
        m_intbitset = mocker.Mock(intbitset, return_value=m_intbitset_inst)
        # if retval is not None:
        #     m_intbitset.return_value = retval

        # Patch
        mocker.patch(path_to_patch, m_intbitset)

        return (m_intbitset, m_intbitset_inst)
    return f_intbitset_

@pytest.fixture
def m_conn(mocker):
    import redis
    m_conn = mocker.Mock(redis.StrictRedis())
    return m_conn

@pytest.fixture
def get_TestRedisWorker(mocker, m_get_record_orig_or_mem):
    def inner(uuid, m_conn):
        from invenio_checker.clients.worker import RedisWorker

        class TestRedisWorker(RedisWorker):  # pylint: disable=missing-docstring
            def __init__(self):  # pylint: disable=super-init-not-called
                pass

        worker = TestRedisWorker()

        worker.uuid = uuid
        worker.conn = m_conn

        worker.get_record_orig_or_mem = m_get_record_orig_or_mem
        return worker
    return inner

@pytest.fixture
def m_get_record_orig_or_mem(mocker):
    """Mock for RedisWorker's get_record_orig_or_mem method."""
    return mocker.Mock(dict())

@pytest.fixture()
def m_request(mocker, m_session):
    m_request_ = mocker.Mock(SubRequest)
    m_request_.session = m_session
    return m_request_

@pytest.fixture()
def m_session(mocker, m_config):
    """pytest session mock to use after session has been initialized."""
    m_session_ = mocker.Mock(Session)
    m_session_.config = m_config
    m_session_.invenio_records = {'original': {},
                                  'modified': {},
                                  'temporary': {}}
    m_session_.invenio_eliot_action = mocker.MagicMock(
        return_value=mocker.MagicMock(spec=file))

    # mocker.patch('invenio_checker.conftest.conftest_checker.Session', m_session_)

    return m_session_

@pytest.fixture()
def m_config(mocker, m_option):
    """pytest config mock to use after config has been initialized."""
    m_config_ = mocker.Mock()
    return m_config_

@pytest.fixture()
def m_option(mocker, m_conn, get_TestRedisWorker):
    """pytest option mock to use after option has been initialized."""
    m_option_ = mocker.Mock()
    # m_option_.redis_worker = get_TestRedisWorker('abc', m_conn)

    return m_option_

# @pytest.fixture()
# def m_option(mocker, m_conn, get_TestRedisWorker):
