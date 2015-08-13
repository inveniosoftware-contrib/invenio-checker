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
import signal
from copy import deepcopy
from collections import defaultdict, namedtuple

import py
import pytest
from _pytest.runner import pytest_runtest_makereport as orig_pytest_runtest_makereport
from _pytest.terminal import TerminalReporter
from six import StringIO
from py._io.terminalwriter import TerminalWriter

import jsonpatch
from functools import wraps, partial
from invenio.ext.sqlalchemy import db as invenio_db
from .redis_helpers import RedisWorker, StatusWorker
from invenio_records.api import get_record as get_record_orig
from invenio.legacy.search_engine import perform_request_search as perform_request_search_orig
from .models import CheckerRule
from .recids import ids_from_input
from intbitset import intbitset  # pylint: disable=no-name-in-module
from orderedset import OrderedSet

from invenio_checker.redis_helpers import get_workers_with_unprocessed_results
# from invenio_checker.supervisor import split_on_conflict


try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

import imp; foo = imp.load_source('foo', '/home/gs-sis/z.py')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# TERMINATION
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def die(rcv_signal, frame):
    raise SystemExit


signal.signal(signal.SIGINT, die)
signal.signal(signal.SIGTERM, die)


def pytest_exception_interact(node, call, report):
    """Terminate execution on SystemExit.

    This is a workaround for the fact that pytest/billiard interpret SIGTERM
    sent to a celery thread to have come from the test function itself. We ask
    pytest to handle this graecfully by raising Interrupted.

    Not calling os._exit() here is important so that we don't break eventlet,
    if in use.

    :type node: :py:class:_pytest.main.Node
    :type call: :py:class:_pytest.runner.CallInfo
    :type report: :py:class:_pytest.runner.TestReport
    """
    if isinstance(call.excinfo.value, SystemExit):
        redis_worker = node.config.option.redis_worker
        warn('Ending worker' + str(redis_worker.task_id))
        redis_worker._cleanup()
        raise node.session.Interrupted(True)


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# COMMUNICATE WITH MASTER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def pytest_collection_modifyitems(session, config, items):
    """Report allowed recids and jsonpaths to master and await start.

    :type session: :py:class:_pytest.main.Session
    :type config: :py:class:_pytest.config.Config
    :type items: list
    """
    unique_functions_found = set((item.function for item in items))
    assert len(unique_functions_found) == 1,\
        "We only support one check function per file. Found {0} instead. "\
        "Don't forget to scroll up for other exceptions!"\
        .format(len(unique_functions_found))
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
    redis_worker = config.option.redis_worker
    redis_master = redis_worker.master

    redis_worker.allowed_paths = allowed_paths
    redis_worker.allowed_recids = allowed_recids

    def worker_conflicts_with_currently_running(worker):
        from .supervisor import _are_compatible
        foreign_running_workers = get_workers_with_unprocessed_results()
        for foreign in foreign_running_workers:
            if not _are_compatible(worker, foreign):
                print 'CONFLICT'
                return True
        return False

    while True:
        redis_master.lock.get()
        if not worker_conflicts_with_currently_running(redis_worker):
            print 'RESUMING ' + str(redis_worker.task_id)
            redis_worker.status = StatusWorker.running
            redis_master.lock.release()
            return
        redis_master.lock.release()
        time.sleep(1)



################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FIXTURES
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


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
def db():
    """Wrap the invenio db instance."""
    return invenio_db


@pytest.fixture(scope="session")
def perform_request_search(request):
    """Wrap `perform_request_search`.

    :type request: :py:class:_pytest.python.SubRequest
    """
    return perform_request_search_orig


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
        invenio_records = request.session.invenio_records
        if recid not in invenio_records['original']:
            invenio_records['original'][recid] = get_record_orig(recid)

        if recid not in invenio_records['modified']:
            invenio_records['modified'][recid] = deepcopy(invenio_records['original'][recid])

        if recid not in invenio_records['temporary']:
            invenio_records['temporary'][recid] = invenio_records['modified'][recid]
        return invenio_records['temporary'][recid]

    return _get_record


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
        # current_function = request.node  #<class '_pytest.python.Function'>
        location_tuple = LocationTuple.from_stack(inspect.stack()[1])
        for reporter in request.config.option.invenio_reporters:
            reporter.report(user_readable_msg, location_tuple)
    return _log


@pytest.fixture(scope="function")
def cfg_args(request):
    """Return arguments given to the task from the database configuration.

    :type request: :py:class:_pytest.python.SubRequest
    """
    return request.config.option.invenio_rule.arguments


@pytest.fixture(scope="function")
def record(request):
    """Return a single record from this batch.

    :type request: :py:class:_pytest.python.SubRequest
    """
    record_id = request.param
    return get_record(request)(record_id)


def pytest_generate_tests(metafunc):
    """Parametrize the check function with `record`.

    :type metafunc: :py:class:_pytest.python.Metafunc
    """
    if 'record' in metafunc.fixturenames:
        metafunc.parametrize("record", batch_recids(metafunc.config),
                             indirect=True)


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# RESULT HANDLING
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def pytest_sessionstart(session):
    """Initialize session-wide variables for record management and caching.

    :type session: :py:class:`_pytest.main.Session`
    """
    session.invenio_records = {'original': {}, 'modified': {}, 'temporary': {}}
    session.invenio_patches = OrderedSet()
    Session.session = session


class Session(object):
    session = None

FullPatch = namedtuple('FullPatch', ['recid', 'record_hash', 'patch'])

def _patches_of_last_execution(action):
    """Get or apply the full_patches generated during the last check.

    If 'apply' is requested, the 'modified' records will be update from the
    'temporary' ones.

    If 'return' is requested, the patches are generated and returned, but never
    applied.

    :type action: str
    :param action: 'apply' or 'return'

    ..note::
        `invenio_records` is populated by the `get_record` function.
    """
    assert action in ('apply', 'return')

    session = Session.session
    invenio_records = session.invenio_records

    def get_full_patches():
        """Return all the record patches resulting from the last run."""
        for recid, modified_record in invenio_records['temporary'].items():
            original_record = invenio_records['original'][recid]
            patch = jsonpatch.make_patch(original_record, modified_record)
            if patch:
                yield FullPatch(recid, hash(original_record), patch)

    if action == 'return':
        for full_patch in get_full_patches():
            del invenio_records['temporary'][full_patch.recid]
            yield full_patch

    elif action == 'apply':
        for recid in invenio_records['temporary'].keys():
            invenio_records['modified'][recid] = invenio_records['temporary'].pop(recid)


# Runs after exception has been reported to the reporter, after every single fine-grained step
def pytest_runtest_logreport(report):
    """
    TODO
    """
    if report.when == 'teardown' and report.outcome == 'passed':
        _patches_of_last_execution('apply')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# OPTIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@lru_cache(maxsize=2)
def _load_rule_from_db(rule_name):
    """Translate the name of the rule set to this task to a database object.

    :type rule_name: str
    """
    return CheckerRule.query.get(rule_name)


def pytest_addoption(parser):
    """Parse arguments given to the command line of this batch.

    :type parser: :py:class:`_pytest.config.Parser`
    """
    parser.addoption("--invenio-rule", action="store", type=_load_rule_from_db,
                     help="get rule", dest='invenio_rule')
    parser.addoption("--invenio-task-id", action="store", type=RedisWorker,
                     help="get task id", dest='redis_worker')
    parser.addoption("--invenio-master-id", action="store", type=str,
                     help="get master id", dest='invenio_master_id')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# REPORTER CALLER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def pytest_sessionfinish(session, exitstatus):
    """whole test run finishes.

    TODO: Upload
    """

    invenio_records = session.invenio_records

    for recid, modified_record in invenio_records['temporary'].items():
        original_record = invenio_records['original'][recid]
        patch = jsonpatch.make_patch(original_record, modified_record)
        if patch:
            yield FullPatch(recid, hash(original_record), patch)

    for patch in session.invenio_patches:
        print "{} {}".format(patch.recid, str(patch.patch))


class LocationTuple(object):

    @staticmethod
    def from_report_location(report_location):
        """Convert a `report_location` to a `LocationTuple`.

        :type report_location: tuple
        """
        fspath, lineno, domain = report_location
        return os.path.abspath(fspath), lineno, domain

    @staticmethod
    def from_stack(stack):
        """Convert a `stack` to a `LocationTuple`.

        :type stack: tuple
        """
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


class InvenioReporter(TerminalReporter):

    ansi_escape = re.compile(r'\x1b[^m]*m')

    def __init__(self, reporter):
        """Initialize TerminalReporter without features we don't need.

        :type reporter: :py:class:`_pytest.terminal.TerminalReporter`
        """
        TerminalReporter.__init__(self, reporter.config)

    @contextmanager
    def new_tw(self):
        """Scoped terminal writer to get output of designated functions.

        ..note:: Will catch any exceptions raised while in the scope and append
        them to the stream. This way one can call deprecated functions and
        actually get a report about it.
        """

        class StrippedStringIO(StringIO):
            """StringIO that strips ansi characters."""
            def write(self, message):
                """Escape all ansi characters from input."""
                message = InvenioReporter.ansi_escape.sub('', message)
                StringIO.write(self, message)  # StringIO is old-style

        tmp_stream = StrippedStringIO()

        old_file = self._tw._file  # pylint: disable=no-member
        self._tw._file = tmp_stream  # pylint: disable=no-member

        def getvalue():
            """Return everything that is in the stream."""
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
        """Report failure during colltion.

        :type report: :py:class:_pytest.runner.CollectReport
        """
        TerminalReporter.pytest_collectreport(self, report)

        if report.failed:
            self.report_failure(report, when='collect')

    def pytest_runtest_logreport(self, report):
        """Report failure during check run.

        :type report: :py:class:_pytest.runner.TestReport
        """
        if hasattr(report, 'wasxfail'):
            return
        if report.failed:
            self.report_failure(report)
        else:
            pass
            # TODO: record checked records to DB

    def pytest_runtest_logstart(self, nodeid, location):
        """No-op terminal-specific prints."""
        pass

    def summary_failures(self):
        """No-op terminal-specific prints."""
        pass

    def summary_errors(self):
        """No-op terminal-specific prints."""
        pass

    def report_failure(self, report, when=None):
        """Dispatch all possible types of failures to enabled reporters.

        :type when: None or str
        :type report: :py:class:_pytest.runner.BaseReport
        """
        when = when or report.when
        assert when in ('collect', 'setup', 'call', 'teardown')

        with self.new_tw() as getvalue:
            self._outrep_summary(report)  # pylint: disable=no-member
        outrep_summary = getvalue()

        # Output, should use celery?
        location_tuple = LocationTuple.from_report_location(report.location)
        try:
            exc_info = (
                report.excinfo.type,
                report.excinfo.value,
                report.excinfo.traceback[0]._rawentry
            )
        except AttributeError:
            exc_info = sys.exc_info()
        formatted_exception = ''.join(traceback.format_exception(*exc_info))

        # Inform all enabled reporters
        patches = tuple(_patches_of_last_execution('return'))
        for reporter in pytest.config.option.invenio_reporters:  # pylint: disable=no-member
            report_exception = partial(reporter.report_exception, when, outrep_summary,
                                       location_tuple, formatted_exception=formatted_exception)
            if patches:
                report_exception(patches=patches)
            else:
                report_exception(patches=patches)

    # def pytest_internalerror(self, excrepr):
    #     return 0

# def pytest_internalerror(excrepr, excinfo):
#     when = 'internal'
#     stack = inspect.getinnerframes(excinfo.tb)
#     location_tuple = LocationTuple.from_stack(stack[1])
#     formatted_exception = ''.join(traceback.format_exception(*excinfo._excinfo))
#     summary = excrepr.reprcrash.message

#     if hasattr(pytest.config.option, 'invenio_reporters'):
#         for reporter in pytest.config.option.invenio_reporters:
#             # reporter.report_exception(when, summary, location_tuple, excinfo._excinfo, formatted_exception)
#             reporter.report_exception(when, summary, location_tuple, formatted_exception=formatted_exception)
#     return 1


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# INTIALIZE, REGISTER REPORTERS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


@pytest.mark.trylast
def pytest_configure(config):
    """Register our report handlers' handler.

    :type config: :py:class:`_pytest.config.Config`
    """
    if hasattr(config, 'slaveinput'):
        return  # xdist slave, we are already active on the master

    # Reporters
    # @lru_cache(maxsize=2)
    def get_reporters(invenio_rule):
        """
        :type invenio_rule: :py:class:`invenio_checker.models.CheckerRule`
        """
        # TODO
        # return [reporter_from_spec(reporter.module, reporter.file)
        #         for reporter in checker_rule.reporters]
        from reporter import get_by_name
        return [get_by_name(1)]

    config.option.invenio_reporters = get_reporters(config.option.invenio_rule)

    # Get the current terminal reporter
    terminalreporter = config.pluginmanager.getplugin('terminalreporter')

    # Unregister it
    config.pluginmanager.unregister(terminalreporter)

    # Add our own to act as a gateway
    invenioreporter = InvenioReporter(terminalreporter)
    config.pluginmanager.register(invenioreporter, 'invenioreporter')


def pytest_runtest_makereport(item, call):
    """Override in order to inject `excinfo` for internalerror.

    :type item: :py:class:`_pytest.python.Function`
    :type call: :py:class:`_pytest.runner.CallInfo`
    """
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
