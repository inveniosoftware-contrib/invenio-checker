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

from invenio.base.wrappers import lazy_import
import inspect
import os
import sys
import re
import traceback
from contextlib import contextmanager
from warnings import warn
from copy import deepcopy

import pytest
from _pytest.runner import pytest_runtest_makereport as orig_pytest_runtest_makereport
from _pytest.terminal import TerminalReporter
from six import StringIO

import jsonpatch
from functools import wraps, partial
Query = lazy_import('invenio_search.api.Query')
from .worker import (
    RedisWorker,
    StatusWorker,
    make_fullpatch,
    get_workers_with_unprocessed_results,
)
import eliot
from eliot import (
    Message,
)
from invenio_checker.worker import workers_are_compatible
from .config import get_eliot_log_path
from .registry import reporters_files
import mock


try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# ELIOT
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def eliotify(func):
    """Decorator that puts `func` in the eliot context of this worker.

    Useful for:
        * Logging in the correct context,
        * Logging exceptions in the wrapped function.

    :type func: callable
    :param func: function to decorate
    """
    @wraps(func)
    def inner(*args, **kwargs):
        """
        :param args: arguments to pass to `func`
        :param kwargs: keyword arguments to pass to `func`
        """
        with Session.session.invenio_eliot_action.context():
            return func(*args, **kwargs)
    return inner

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# TERMINATION
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_internalerror(excrepr, excinfo):
    """Called when an internalerror is raised inside pytest."""
    _pytest_internalerror(excrepr, excinfo)

@eliotify
def _pytest_internalerror(excrepr, excinfo):
    """Log an internal error and terminate logging action."""
    with mock.patch('eliot._traceback.sys.exc_info',
                    mock.Mock(return_value=(excinfo.type, excinfo.value, excinfo.tb))):
        eliot.write_traceback()
    exception = excinfo.value
    Session.session.invenio_eliot_action.finish(exception)

#def pytest_keyboard_interrupt(excinfo):

def pytest_exception_interact(node, call, report):
    """Called when a potentially handle-able exception is raised."""
    _pytest_exception_interact(node, call, report)

@eliotify
def _pytest_exception_interact(node, call, report):
    """Terminate execution on SystemExit.

    This is a workaround for the fact that pytest/billiard interpret SIGTERM
    sent to a celery thread to have come from the test function itself. We ask
    pytest to handle this gracefully by raising Interrupted.

    Not calling os._exit() here is important so that we don't break eventlet,
    if in use.

    :type node: :py:class:_pytest.main.Node
    :type call: :py:class:_pytest.runner.CallInfo
    :type report: :py:class:_pytest.runner.TestReport
    """
    excinfo = call.excinfo
    with mock.patch('eliot._traceback.sys.exc_info',
                    mock.Mock(return_value=(excinfo.type, excinfo.value, excinfo.tb))):
        eliot.write_traceback()
    exception = call.excinfo.value
    Session.session.invenio_eliot_action.finish(exception)
    if isinstance(exception, SystemExit):
        redis_worker = node.config.option.redis_worker
        redis_worker.status = StatusWorker.failed

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# COMMUNICATE WITH MASTER
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def ensure_only_one_test_function_exists_in_check(items):
    """Raise if != 1 check funcs were collected from a single check file."""
    unique_functions_found = {item.function for item in items}

    if not unique_functions_found:
        raise AssertionError(
            "No check functions were found."
            " Scroll up for exceptions that may have prevented collection!"
        )
    elif not len(unique_functions_found) == 1:
        raise AssertionError(
            "We support one check function per file. Found {0} instead."
            .format(len(unique_functions_found))
        )

def get_restrictions_from_check_class(item, task_arguments, session):
    """Get performance hints from this check.
    TODO
    """
    if hasattr(item, 'cls'):
        if hasattr(item.cls, 'allowed_paths'):
            allowed_paths = item.cls.allowed_paths(task_arguments)
        else:
            allowed_paths = set()
        if hasattr(item.cls, 'allowed_recids'):
            allowed_recids = item.cls.allowed_recids(
                task_arguments,
                batch_recids(session),
                all_recids(session),
                search(session)
            )
        else:
            allowed_recids = batch_recids(session)

    if allowed_recids - all_recids(session):
        raise AssertionError('Check requested recids that are not in the'
                             ' database!')

    return allowed_paths, allowed_recids

def worker_conflicts_with_currently_running(worker):
    """Get the workers this worker would conflict with if it ran now.

    :param worker: worker that we wish to start.
    :type worker: RedisWorker

    :returns: [RedisWorker]
    """
    foreign_running_workers = get_workers_with_unprocessed_results()
    blockers = set()
    for foreign in foreign_running_workers:
        if not workers_are_compatible(worker, foreign):
            blockers.add(foreign)
    return blockers

def pytest_collection_modifyitems(session, config, items):
    """Call when pytest has finished collecting items."""
    _pytest_collection_modifyitems(session, config.option.invenio_rule.arguments,
                                   config.option.redis_worker, items)

@eliotify
def _pytest_collection_modifyitems(session, task_arguments, worker, items):
    """Report allowed recids and jsonpaths to master and await start.

    :type session: :py:class:_pytest.main.Session
    :type config: :py:class:_pytest.config.Config
    :type items: list
    """
    ensure_only_one_test_function_exists_in_check(items)
    item = items[0]
    worker.allowed_paths, worker.allowed_recids = \
        get_restrictions_from_check_class(item, task_arguments, session)
    worker.status = StatusWorker.ready  # XXX unused?

    with worker.lock():
        blockers = worker_conflicts_with_currently_running(worker)
        if blockers:
            Message.log(message_type='found conflicting workers', value=str(blockers))
            # print 'CONFLICT {} {}'.format(worker.uuid, blockers)
            worker.retry_after_ids = {bl.uuid for bl in blockers}
            del items[:]
        else:
            # print 'RESUMING ' + str(worker.uuid)
            worker.status = StatusWorker.running


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
def search(request):
    """Wrap `Query(request).search()`.

    :type request: :py:class:_pytest.python.SubRequest
    """
    def _query(query):
        """
        :type query: str
        """
        ret = Query(query).search()
        ret.records = (get_record(request)(recid) for recid in ret.recids)
        return ret
    return _query


@pytest.fixture(scope="session")
def arguments(request):
    """Get the user-set arguments from the database."""
    return request.config.option.invenio_rule.arguments


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
    @eliotify
    def _log(user_readable_msg):
        location_tuple = LocationTuple.from_report_location(request.node.reportinfo())
        Message.log(message_type="check log", msg=user_readable_msg, location=location_tuple)
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
    """Called at the beginning of the session, after config initialization.

    :type session: :py:class:`_pytest.main.Session`
    """
    return _pytest_sessionstart(session)


def _pytest_sessionstart(session):
    """Initialize session-wide variables for record management and caching."""
    session.invenio_records = {'original': {}, 'modified': {}, 'temporary': {}}
    Session.session = session

    # Set the eliot log path
    config = session.config
    worker = config.option.redis_worker
    master = config.option.redis_worker.master
    eliot.to_file(open(os.path.join(get_eliot_log_path(),
                                    master.uuid + '.' + worker.uuid), "ab"))
    session.invenio_eliot_action = eliot.start_action(action_type=u"pytest worker")


class Session(object):
    session = None


def get_fullpatches_of_last_run(step):
    """Return all the record patches resulting from the last run.

    ..note::
        `invenio_records` is populated by the `get_record` function.
    """
    session = Session.session
    invenio_records = session.invenio_records
    redis_worker = session.config.option.redis_worker

    assert step in ('temporary', 'modified')
    for recid, modified_record in invenio_records[step].items():
        original_record = invenio_records['original'][recid]
        patch = jsonpatch.make_patch(original_record, modified_record)
        if patch:
            record_hash = 'FIXME'
            yield make_fullpatch(recid,
                                 record_hash,
                                 patch.to_string(),
                                 redis_worker.task_id)


def pytest_runtest_logreport(report):
    """Process a report of a the setup/call/teardown phase of execution.

    ..note::
        Called after exceptions have been reported.
    """
    return _pytest_runtest_logreport(report)


def _pytest_runtest_logreport(report):
    """
    Move the 'temporary' keys that were passed to the worker to 'modified'
    if the test was successful.
    """
    session = Session.session
    invenio_records = session.invenio_records

    if report.when == 'teardown' and report.outcome == 'passed':
        temp_keys = invenio_records['temporary'].keys()
        for recid in temp_keys:
            invenio_records['modified'][recid] = invenio_records['temporary'].pop(recid)


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# OPTIONS
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def pytest_addoption(parser):
    """Parse arguments given to the command line of this batch.

    :type parser: :py:class:`_pytest.config.Parser`
    """
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
    """Called when the entire check run has completed.

    TODO?: Upload
    """
    worker = session.config.option.redis_worker
    invenio_records = session.invenio_records
    Session.session.invenio_eliot_action.finish()
    return _pytest_sessionfinish(session, worker, invenio_records, exitstatus)

@eliotify
def _pytest_sessionfinish(session, worker, invenio_records, exitstatus):
    """Push the stored patches to redis."""
    # TODO: Should we check exitstatus here?
    idx = 0
    for idx, fullpatch in enumerate(get_fullpatches_of_last_run('modified'), 1):
        Message.log(message_type='adding patch to redis', patch=fullpatch)
        worker.patch_to_redis(fullpatch)
    Message.log(message_type='patch to redis summary', patch_count=idx)


class LocationTuple(object):
    """Common structure to identify the location of some code.

    This is useful for sending the same kind of tuple to the reporters, no
    matter what it is that we are reporting.

    The format is: (absolute_path, line number, domain)
    """

    @staticmethod
    def from_report_location(report_location):
        """Convert a `report_location` to a `LocationTuple`.

        :type report_location: tuple
        """
        fspath, lineno, domain = report_location
        return os.path.abspath(str(fspath)), lineno, domain


class InvenioReporter(TerminalReporter):
    """TODO"""

    ansi_escape = re.compile(r'\x1b[^m]*m')

    def __init__(self, reporter):
        """Initialize TerminalReporter without features we don't need.

        :type reporter: :py:class:`_pytest.terminal.TerminalReporter`
        """
        TerminalReporter.__init__(self, reporter.config)

    @contextmanager
    def new_tw(self):
        """Scoped terminal writer to get output of designated functions.

        ..note::
            Will catch any exceptions raised while in the scope and append
            them to the stream. This way one can call deprecated functions and
            actually get a report about it.
        """

        class StrippedStringIO(StringIO):
            """StringIO that strips ansi characters."""
            def write(self, message):
                message = InvenioReporter.ansi_escape.sub('', message)
                StringIO.write(self, message)  # StringIO is old-style

        tmp_stream = StrippedStringIO()

        # XXX Remember to feel bad about this.
        old_file = self._tw._file  # pylint: disable=no-member
        self._tw._file = tmp_stream  # pylint: disable=no-member

        def getvalue():
            """Return everything that is in the stream."""
            tmp_stream.seek(0)
            return tmp_stream.getvalue()

        exc_info = None
        try:
            yield getvalue
        except Exception:  # pylint: disable=broad-except
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

        # Output, should use celery? XXX
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

        # TODO: eliotify
        # Remove ambiguous patches and inform all enabled reporters
        invenio_records = Session.session.invenio_records
        patches = []
        for fullpatch in get_fullpatches_of_last_run('temporary'):
            del invenio_records['temporary'][fullpatch['recid']]
            patches.append(fullpatch)

        for reporter in pytest.config.option.invenio_reporters:  # pylint: disable=no-member
            report_exception = partial(
                reporter.report_exception,
                when,
                outrep_summary,
                location_tuple,
                formatted_exception=formatted_exception
            )
            if patches:
                report_exception(patches=patches)
            else:
                report_exception()

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
    def get_reporters(invenio_rule):
        return [reporter.module.get_reporter(config.option.invenio_rule.name)
                for reporter in invenio_rule.reporters]

    # Collect other useful variables
    master = config.option.redis_worker.master
    config.option.invenio_execution = master.get_execution()
    config.option.invenio_rule = config.option.invenio_execution.rule
    config.option.invenio_reporters = get_reporters(config.option.invenio_rule)

    # Unregister the current terminal reporter
    terminalreporter = config.pluginmanager.getplugin('terminalreporter')
    config.pluginmanager.unregister(terminalreporter)
    # Add our own to act as a gateway for our own reporting system
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
