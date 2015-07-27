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

import inspect
import os
import sys
import re
import traceback
from contextlib import contextmanager
import time
from warnings import warn

import py
import pytest
from _pytest.runner import pytest_runtest_makereport as orig_pytest_runtest_makereport
from _pytest.terminal import TerminalReporter
from six import StringIO
from py._io.terminalwriter import TerminalWriter

from .redis_helpers import RedisWorker, StatusWorker
from invenio_records.api import get_record as get_record_orig
from invenio.legacy.search_engine import perform_request_search as perform_request_search_orig
from .models import CheckerRule
from .recids import ids_from_input
from intbitset import intbitset


try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache


ansi_escape = re.compile(r'\x1b[^m]*m')


class LocationTuple(object):

    @staticmethod
    def from_report_location(report_location):
        fspath, lineno, domain = report_location
        return os.path.abspath(fspath), lineno, domain

    @staticmethod
    def from_stack(stack):
        frame, filename, line_number, function_name, lines, index = stack
        function_name = frame.f_code.co_name  # 'check_fail'
        try:
            argvalues = inspect.getargvalues(frame)
            first_argument = argvalues.locals[argvalues.args[0]]
            class_name = first_argument.__class__.__name__  # CheckWhatever
        except IndexError:
            domain = function_name
        else:
            domain = '{0}.{1}'.format(class_name, function_name)
        return filename, line_number, domain


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# COMMUNICATE WITH MASTER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


# TODO: Do this before the collection ( dry run )
def pytest_collection_modifyitems(session, config, items):
    """ called after collection has been performed, may filter or re-order
    the items in-place."""

    assert len(set((item.function for item in items))) == 1, \
        'We only support one check function per file.'
    item = items[0]

    # Set allowed_paths and allowed_recids
    if hasattr(item, 'cls'):
        if hasattr(item.cls, 'allowed_paths'):
            # TODO Must return jsonpointers (IETF RFC 6901)
            allowed_paths = item.cls.allowed_paths(config.option.invenio_rule.arguments)
        else:
            allowed_paths = set()
        if hasattr(item.cls, 'allowed_recids'):
            allowed_recids = item.cls.allowed_recids(config.option.invenio_rule.arguments,
                                                     batch_recids(session),
                                                     all_recids(session),
                                                     perform_request_search(session))
        else:
            allowed_recids = batch_recids(session)

    # We could be intersecting instead of raising, but we are evil.
    if allowed_recids - all_recids(session):
        raise Exception('Check requested recids that are not in the database!')

    config.redis_worker.allowed_paths = allowed_paths
    config.redis_worker.allowed_recids = allowed_recids

    # Tell master that we are ready and wait for further instructions
    # TODO
    # We could wait for worker to initialize here. We are extremely unlikely to
    # run into this condition. If we don't do this, worst case we timeout.
    # Should be easy with the intervalometer.
    config.redis_worker.sub_to_master()
    ctx_receivers = config.redis_worker.pub_to_master('ready')
    if not ctx_receivers:
        raise RuntimeError('Master has gone away!')

    while True:
        message = config.redis_worker.pubsub.get_message()
        if message:
            if message['data'] == 'run':
                # No need to set status, master does that within the Lock.
                return
            elif message['data'] == 'cancel':
                # Be graceful
                del items[:]
                return
            else:
                raise ValueError('Unknown message received: {0}'.format(message))
        time.sleep(0.5)


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FIXTURES
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@pytest.fixture(scope="session")
def db():
    return db


@pytest.fixture(scope="session")
def perform_request_search(request):
    return perform_request_search_orig


@pytest.fixture(scope="session")
def get_record(request):
    return get_record_orig


@pytest.fixture(scope="session")
def all_recids(request):
    try:
        config = request.config
    except AttributeError:
        config = request
    master_id = config.option.invenio_master_id
    ret = config.redis_worker.get_master_all_recids(master_id)
    if not ret:
        warn("Master's all_recids is empty!")
    return ret


@pytest.fixture(scope="session")
def batch_recids(request):
    try:
        config = request.config
    except AttributeError:
        config = request
    if hasattr(config.option, 'bundle_requested_recids'):
        bundle_requested_recids = config.option.bundle_requested_recids
    else:
        bundle_requested_recids = intbitset(trailing_bits=True)

    modified_requested_recids = config.option.invenio_rule.modified_requested_recids
    if not modified_requested_recids:
        warn('modified_requested_recids is empty!')

    all_recids_ = all_recids(request)
    if not all_recids_:
        warn('all_recids is empty!')

    if not bundle_requested_recids:
        warn('bundle_requested_recids is empty!')

    ret = modified_requested_recids & \
        all_recids_ & bundle_requested_recids

    if not ret:
       warn('Record ID intersection returned no records!')

    return ret


@pytest.fixture(scope="function")
def log(request):
    def _log(user_readable_msg):
        # current_function = request.node  #<class '_pytest.python.Function'>
        location_tuple = LocationTuple.from_stack(inspect.stack()[1])
        for reporter in request.config.option.invenio_reporters:
            reporter.report(user_readable_msg, location_tuple)
    return _log


@pytest.fixture(scope="function")
def cfg_args(request):
    return request.config.option.invenio_rule.arguments


@pytest.fixture(scope="function")
def record(request):
    record_id = request.param
    return record_id  # TODO: Remove for release
    return get_record_orig(record_id)


def pytest_generate_tests(metafunc):
    # Unfortunately this runs before `pytest_runtest_setup`, so we have to
    # extract the records again
    if 'record' in metafunc.fixturenames:
        metafunc.parametrize("record",
                             batch_recids(metafunc.config),
                             indirect=True)


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# OPTIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@lru_cache(maxsize=2)
def _load_rule_from_db(rule_name):
    return CheckerRule.query.get(rule_name)


def pytest_addoption(parser):
    parser.addoption("--invenio-bundle-requested-recids", action="store", type=ids_from_input,
                     help="set records", dest='bundle_requested_recids')
    parser.addoption("--invenio-rule", action="store", type=_load_rule_from_db,
                     help="get rule", dest='invenio_rule')
    parser.addoption("--invenio-task-id", action="store", type=str,
                     help="get task id", dest='invenio_task_id')
    parser.addoption("--invenio-master-id", action="store", type=str,
                     help="get master id", dest='invenio_master_id')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# REPORTER CALLER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


class InvenioReporter(TerminalReporter):
    def __init__(self, reporter):
        TerminalReporter.__init__(self, reporter.config)

    @contextmanager
    def new_tw(self):
        """Scoped terminal writer to get output of designated functions.

        ..note:: Will catch any exceptions raised while in the scope and append
        them to the stream. This way one can call deprecated functions and
        actually get a report about it.
        """

        class StrippedStringIO(StringIO):
            def write(self, message):
                message = ansi_escape.sub('', message)
                StringIO.write(self, message)  # StringIO is old-style

        tmp_stream = StrippedStringIO()

        old_file = self._tw._file  # pylint: disable=no-member
        self._tw._file = tmp_stream  # pylint: disable=no-member

        def getvalue():
            tmp_stream.seek(0)
            return tmp_stream.getvalue()

        exc_info = None
        try:
            yield getvalue
        except Exception:
            exc_info = sys.exc_info()
        finally:
            if exc_info:
                formatted_exception = ''.join(traceback.format_exception(*exc_info))
                tmp_stream.write('\nException raised while collecting description:\n')
                tmp_stream.write(formatted_exception)
            self._tw._file = old_file  # pylint: disable=no-member


    def pytest_collectreport(self, report):
        TerminalReporter.pytest_collectreport(self, report)

        if report.failed:
            self.report_failure(report, when='collect')

    def pytest_runtest_logreport(self, report):
        if hasattr(report, 'wasxfail'):
            return
        if report.failed:
            self.report_failure(report)
        else:
            pass
            # TODO: record checked records to DB

    def pytest_runtest_logstart(self, nodeid, location):
        pass

    def summary_failures(self):
        pass

    def summary_errors(self):
        pass

    def report_failure(self, report, when=None):
        when = when or report.when
        assert when in ('collect', 'setup', 'call', 'teardown')

        with self.new_tw() as getvalue:
            self._outrep_summary(report)  # pylint: disable=no-member
        outrep_summary = getvalue()

        # Output, should use celery?
        location_tuple = LocationTuple.from_report_location(report.location)
        # exc_info = (
        #     report.excinfo.type,
        #     report.excinfo.value,
        #     report.excinfo.traceback[0]._rawentry
        # )
        # formatted_exception = ''.join(traceback.format_exception(*exc_info))

        # Inform all enabled reporters
        for reporter in pytest.config.option.invenio_reporters:
            reporter.report_exception(when, outrep_summary, location_tuple)
            # reporter.report_exception(when, outrep_summary, location_tuple, exc_info, formatted_exception)

    def pytest_internalerror(self, excrepr):
        return 0

def pytest_internalerror(excrepr, excinfo):
    when = 'internal'
    stack = inspect.getinnerframes(excinfo.tb)
    location_tuple = LocationTuple.from_stack(stack[1])
    formatted_exception = ''.join(traceback.format_exception(*excinfo._excinfo))
    summary = excrepr.reprcrash.message

    if hasattr(pytest.config.option, 'invenio_reporters'):
        for reporter in pytest.config.option.invenio_reporters:
            # reporter.report_exception(when, summary, location_tuple, excinfo._excinfo, formatted_exception)
            reporter.report_exception(when, summary, location_tuple)
    return 1


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# INTIALIZE, REGISTER REPORTERS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@pytest.mark.trylast
def pytest_configure(config):
    """Register our report handlers' handler."""
    if hasattr(config, 'slaveinput'):
        return  # xdist slave, we are already active on the master

    # Redis
    config.redis_worker = RedisWorker(config.option.invenio_task_id)

    # Reporters
    # @lru_cache(maxsize=2)
    def get_reporters(invenio_rule):
        # TODO
        # return [reporter_from_spec(reporter.module, reporter.file)
        #         for reporter in checker_rule.reporters]
        from reporter import get_by_name
        return [get_by_name(1)]

    config.option.invenio_reporters = get_reporters(config.option.invenio_rule)

    # Get the current terminal reporter
    terminalreporter = config.pluginmanager.getplugin('terminalreporter')

    # Unregister it  # TODO: Comment this line for production
    config.pluginmanager.unregister(terminalreporter)

    # Add our own to act as a gateway
    invenioreporter = InvenioReporter(terminalreporter)
    config.pluginmanager.register(invenioreporter, 'invenioreporter')


# def pytest_runtest_makereport(item, call):
#     """Override in order to inject `excinfo`."""
#     excinfo = call.excinfo
#     try:
#         result = orig_pytest_runtest_makereport(item, call)
#     finally:
#         result.excinfo = excinfo
#     return result


# Namespace manipulation
# class InvenioStorage(object):
#     def __init__(self):
#         self.records = None
#         self.reporters = None

#     @property
#     def records(self):
#         return pytest.config.

# def pytest_namespace():
#     pass
#     # pytest has special handling for dicts, so we use a custom class instead
#     # invenio_storage = InvenioStorage()
#     # return {'invenio_storage': invenio_storage}


# @pytest.mark.trylast
# def pytest_cmdline_main(config):
#     # Get the marker
#     import ipdb; ipdb.set_trace()
#     pytest.invenio_storage.records = config.option.records.split(',')
#     pytest.invenio_storage.reporters = config.option.reporters.split(',')

# Results
# def pytest_exception_interact(node, call, report):
#     import ipdb; ipdb.set_trace()
#     report.when # setup, call
#     InvenioReporter.report_failure(report)
