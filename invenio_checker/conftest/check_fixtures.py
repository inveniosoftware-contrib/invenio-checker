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

import pytest
from copy import deepcopy
from invenio_search.api import Query
from warnings import warn
from functools import wraps
from .check_helpers import LocationTuple

def _warn_if_empty(func):
    """Print a warning if the given functions returns no results.

    ..note:: pytest relies on the signature function to set fixtures, in this
    case `request`.

    :type func: callable
    """
    @wraps(func)
    def _warn_if_empty(request):
        """
        :type request: :py:class:_pytest.python.SubRequest
        """
        ret = func(request)
        if not ret:
            warn(func.__name__ + " returned an empty set!")
        return ret
    return _warn_if_empty

def _request_to_config(request_or_config):
    """Resolve pytest config.

    This is useful to make a function that, due to pytest, expects `request`,
    work when called called from pytest itself or from a function that only had
    access to `config`.
    """
    try:
        return request_or_config.config
    except AttributeError:
        return request_or_config

@pytest.fixture(scope="session")
def get_record(request):
    """Wrap `get_record` for record patch generation.

    This function ensures that we
        1) hit the database once per record,
        2) maintain the latest, valid, modified version of the records,
        3) return the same 'temporary' object reference per check.

    :type request: :py:class:`_pytest.python.SubRequest`
    """
    def _get_record(recid):
        redis_worker = request.session.config.option.redis_worker
        invenio_records = request.session.invenio_records
        if recid not in invenio_records['original']:
            invenio_records['original'][recid] = redis_worker.get_record_orig_or_mem(recid)

        if recid not in invenio_records['modified']:
            invenio_records['modified'][recid] = deepcopy(invenio_records['original'][recid])

        if recid not in invenio_records['temporary']:
            invenio_records['temporary'][recid] = invenio_records['modified'][recid]
        return invenio_records['temporary'][recid]
    return _get_record

@pytest.fixture(scope="session")
def search(request, get_record):
    """Wrap `Query(request).search()`.

    :type request: :py:class:_pytest.python.SubRequest
    """
    def _query(query):
        """
        :type query: str

        ..note::
            `get_record` is used so that `invenio_records` is kept up to date.
        """
        ret = Query(query).search()
        ret.records = (get_record(recid) for recid in ret.recids)
        return ret
    return _query

@pytest.fixture(scope="session")
def arguments(request):
    """Get the user-set arguments from the database."""
    return request.config.option.invenio_rule.arguments

@pytest.fixture(scope="session")
@_warn_if_empty
def all_recids(request):
    """Return all the recids this run is ever allowed to change.

    :type request: :py:class:_pytest.python.SubRequest
    """
    config = _request_to_config(request)
    return config.option.redis_worker.master.all_recids

@pytest.fixture(scope="session")
@_warn_if_empty
def batch_recids(request):
    """Return the recids that were assigned to this worker.

    :type request: :py:class:_pytest.python.SubRequest

    :rtype: intbitset
    """
    config = _request_to_config(request)
    return config.option.redis_worker.bundle_requested_recids

@pytest.fixture(scope="function")
def log(request):
    """Wrap a logging function that informs the enabled reporters.

    :type request: :py:class:_pytest.python.SubRequest
    """
    def _log(user_readable_msg):
        from .conftest2 import report_log
        location_tuple = LocationTuple.from_report_location(request.node.reportinfo())
        report_log(request.config.option.invenio_reporters, user_readable_msg, location_tuple)
    return _log

@pytest.fixture(scope="function")
def cfg_args(request):
    """Return arguments given to the task from the database configuration.

    :type request: :py:class:_pytest.python.SubRequest
    """
    return request.config.option.invenio_rule.arguments

@pytest.fixture(scope="function")
def record(request, get_record):
    """Return a single record from this batch.

    :type request: :py:class:_pytest.python.SubRequest
    """
    return get_record(request.param)

def pytest_generate_tests(metafunc):
    """Parametrize the check function with `record`.

    :type metafunc: :py:class:_pytest.python.Metafunc
    """
    if 'record' in metafunc.fixturenames:
        metafunc.parametrize("record", batch_recids(metafunc.config),
                             indirect=True)
