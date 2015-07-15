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

import pytest
from _pytest.runner import pytest_runtest_makereport as orig_pytest_runtest_makereport
from _pytest.terminal import TerminalReporter
from six import StringIO
from py._io.terminalwriter import write_out

from invenio.modules.records.api import get_record as get_record_orig
from invenio.legacy.search_engine import perform_request_search as perform_request_search_orig
from .models import CheckerRule
from .recids import ids_from_input


try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache


ansi_escape = re.compile(r'\x1b[^m]*m')


# TODO
# Helpers
# @lru_cache(maxsize=2)
def get_reporters(invenio_rule):
    # TODO
    # return [reporter_from_spec(reporter.module, reporter.file)
    #         for reporter in checker_rule.reporters]
    from reporter import get_by_name
    return [get_by_name(1)]


@lru_cache(maxsize=2)
def _load_rule_from_db(rule_name):
    return CheckerRule.query.get(rule_name)


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


# Bare fixtures
@pytest.fixture(scope="session")
def db():
    return db
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# COMMUNICATE WITH MASTER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@pytest.fixture(scope="session")
def requested_records(request):
    return request.config.option.invenio_records
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
                                                     requested_recids(session),
                                                     all_recids(session),
                                                     perform_request_search(session))
        else:
            allowed_recids = requested_recids(session)

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
        raise Exception('Master has gone away!')

    worker_timeout = 20  # TODO: From config
    sleep_per_interval = 0.01
    time_slept = 0
    while True:
        message = config.redis_worker.pubsub.get_message()
        if message:
            if message['data'] == 'run':
                return
            elif message['data'] == 'cancel':
                # Be graceful
                del items[:]
                return
            else:
                raise Exception('Unknown message received: {0}'.format(message))
        time.sleep(sleep_per_interval)
        time_slept += sleep_per_interval
        if time_slept >= worker_timeout:
            raise Exception('Master timed out!')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FIXTURES
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


# FIXME: How do we become aware of modifications? FIXME


@pytest.fixture(scope="session")
def perform_request_search(request):
    return perform_request_search_orig


@pytest.fixture(scope="session")
def get_record(request):
    return get_record_orig


@pytest.fixture(scope="session")
def all_record_ids(request):
    from invenio.modules.records.models import Record
    return Record.allids()


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


# Parametrization
def pytest_generate_tests(metafunc):
    # Unfortunately this runs before `pytest_runtest_setup`, so we have to
    # extract the records again
    if 'record' in metafunc.fixturenames:
        metafunc.parametrize("record",
                             [a for a in metafunc.config.option.invenio_records],
                             indirect=True)


@pytest.fixture(scope="function")
def record(request):
    return  # TODO: Remove for release
    record_id = request.param
    return get_record_orig(record_id)


# Options
def pytest_addoption(parser):
    # Add env variable. Use `-E whatever` to set
    parser.addoption("--invenio-records", action="store", type=ids_from_input,
                     help="set records", dest='invenio_records')
    parser.addoption("--invenio-rule", action="store", type=_load_rule_from_db,
                     help="get rule", dest='invenio_rule')


# Configure
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

        # import ipdb; ipdb.set_trace()
        # report_toterminal(report)
        with self.new_tw() as getvalue:
            self._outrep_summary(report)  # pylint: disable=no-member
        outrep_summary = getvalue()

        # Output, should use celery?
        location_tuple = LocationTuple.from_report_location(report.location)
        exc_info = (
            report.excinfo.type,
            report.excinfo.value,
            report.excinfo.traceback[0]._rawentry
        )
        formatted_exception = ''.join(traceback.format_exception(*exc_info))

        # Inform all enabled reporters
        for reporter in pytest.config.option.invenio_reporters:
            reporter.report_exception(outrep_summary, location_tuple, exc_info, formatted_exception)


@pytest.mark.trylast
def pytest_configure(config):
    """Register our report handlers' handler."""

    config.option.invenio_reporters = get_reporters(config.option.invenio_rule)

    if hasattr(config, 'slaveinput'):
        return  # xdist slave, we are already active on the master

    # Get the current terminal reporter
    standard_reporter = config.pluginmanager.getplugin('terminalreporter')

    # Unregister it
    config.pluginmanager.unregister(standard_reporter)

    # Add our own to act as a gateway
    invenioreporter = InvenioReporter(standard_reporter)
    config.pluginmanager.register(invenioreporter, 'invenioreporter')


def pytest_runtest_makereport(item, call):
    """Override in order to inject `excinfo`."""
    excinfo = call.excinfo
    try:
        result = orig_pytest_runtest_makereport(item, call)
    finally:
        result.excinfo = excinfo
    return result

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
