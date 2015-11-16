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

from invenio_base.wrappers import lazy_import
import sys
import re
import traceback
from contextlib import contextmanager

import pytest
from _pytest.runner import pytest_runtest_makereport as orig_pytest_runtest_makereport
from _pytest.terminal import TerminalReporter
from six import StringIO

from .check_fixtures import *  # pylint: disable=wildcard-import
import jsonpatch
from functools import wraps, partial
Query = lazy_import('invenio_search.api.Query')
from invenio_checker.clients.worker import (
    RedisWorker,
    StatusWorker,
    make_fullpatch,
    get_workers_with_unprocessed_results,
    workers_are_compatible,
)
import eliot
from eliot import (
    Message,
)
import mock
from invenio_checker.config import get_eliot_log_file
from .check_helpers import LocationTuple
from _pytest.main import EXIT_OK
from intbitset import intbitset  # pylint: disable=no-name-in-module

Session = None

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# logging
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
        with Session.invenio_eliot_action.context():
            return func(*args, **kwargs)
    return inner

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_addoption
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_addoption(parser):
    """Parse arguments given to the command line of this batch.

    :type parser: :py:class:`_pytest.config.Parser`
    """
    parser.addoption("--invenio-task-id", action="store", type=RedisWorker,
                     help="get task id", dest='redis_worker')


################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_configure
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

@pytest.mark.trylast
def pytest_configure(config):
    """Register our report handlers' handler.

    :type config: :py:class:`_pytest.config.Config`
    """
    # Collect other useful variables
    master = config.option.redis_worker.master
    config.option.invenio_execution = master.get_execution()
    config.option.invenio_rule = config.option.invenio_execution.rule
    config.option.invenio_reporters = []  # Do not load yet

    # Unregister the current terminal reporter
    terminalreporter = config.pluginmanager.getplugin('terminalreporter')
    config.pluginmanager.unregister(terminalreporter)
    # Add our own to act as a gateway for our own reporting system
    invenioreporter = InvenioReporter(terminalreporter)
    config.pluginmanager.register(invenioreporter, 'invenioreporter')

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_collection_modifyitems
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_collection_modifyitems(session, config, items):
    """Call when pytest has finished collecting items."""
    invenio_rule = session.config.option.invenio_rule
    option = session.config.option
    _pytest_collection_modifyitems(config.option.invenio_rule.arguments,
                                   config.option.redis_worker, items,
                                   invenio_rule, option, batch_recids(session),
                                   all_recids(session))

@eliotify
def _pytest_collection_modifyitems(task_arguments, worker, items, invenio_rule,
                                   option, batch_recids_, all_recids_):
    """Report allowed recids and jsonpaths to master and await start.

    :type config: :py:class:_pytest.config.Config
    :type items: list
    """
    # Make sure the check file is sane
    _ensure_only_one_test_function_exists_in_check(items)

    # Inform the worker
    worker.allowed_paths, worker.allowed_recids = \
        _get_restrictions_from_check_class(items[0], task_arguments, batch_recids_, all_recids_)
    worker.status = StatusWorker.ready  # XXX unused?

    # Check if there are conflicts with other workers
    with worker.lock():
        blockers = _worker_conflicts_with_currently_running(worker)
        worker.retry_after_ids = {bl.uuid for bl in blockers}
        if blockers:
            Message.log(message_type='detected conflicting workers', value=str(blockers))
            del items[:]
        else:
            option.invenio_reporters = load_reporters(invenio_rule)
            worker.status = StatusWorker.running

def _ensure_only_one_test_function_exists_in_check(items):
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

def _get_restrictions_from_check_class(item, task_arguments, batch_recids_, all_recids_):
    """Get performance hints from this check.

    TODO
    """
    if hasattr(item, 'cls'):

        # allowed_paths
        if hasattr(item.cls, 'allowed_paths'):
            allowed_paths = item.cls.allowed_paths(task_arguments)
        else:
            allowed_paths = set()

        # allowed_recids
        if hasattr(item.cls, 'allowed_recids'):
            # intbitset segfaults if rhs is not one-of(set, intbitset) later on
            allowed_recids = intbitset(
                item.cls.allowed_recids(
                    task_arguments,
                    batch_recids_,
                    all_recids_,
                ))
        else:
            allowed_recids = batch_recids_

    if allowed_recids - all_recids_:
        raise AssertionError('Check requested recids that are not in the '
                             'database (as detected by master)!')

    return allowed_paths, allowed_recids

def _worker_conflicts_with_currently_running(worker):
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

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_sessionstart
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_sessionstart(session):
    """Called at the beginning of the session, after config initialization.

    :type session: :py:class:`_pytest.main.Session`
    """
    worker = session.config.option.redis_worker
    return _pytest_sessionstart(session, worker)

def _pytest_sessionstart(session, worker):
    """Initialize session-wide variables for record management and caching."""
    session.invenio_records = {'original': {}, 'modified': {}, 'temporary': {}}
    global Session  # pylint: disable=global-statement
    Session = session

    # Set the eliot log path
    eliot.to_file(get_eliot_log_file(worker_id=worker.uuid))
    session.invenio_eliot_action = eliot.start_action(action_type=u"pytest worker")

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_runtest_logreport
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_runtest_logreport(report):
    """Process a report of a the setup/call/teardown phase of execution.

    ..note::
        Called after exceptions have been reported.
    """
    invenio_records = Session.invenio_records
    return _pytest_runtest_logreport(report, invenio_records)

def _pytest_runtest_logreport(report, invenio_records):
    """
    Move the 'temporary' keys that were passed to the worker to 'modified'
    if the test was successful.
    """
    if report.when == 'teardown' and report.outcome == 'passed':
        temp_keys = invenio_records['temporary'].keys()
        for recid in temp_keys:
            invenio_records['modified'][recid] = invenio_records['temporary'].pop(recid)

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_sessionfinish
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################


def pytest_sessionfinish(session, exitstatus):
    """Called when the entire check run has completed.

    TODO?: Upload
    """
    worker = session.config.option.redis_worker
    invenio_reporters = session.config.option.invenio_reporters
    invenio_eliot_action = session.invenio_eliot_action
    invenio_records = session.invenio_records
    return _pytest_sessionfinish(invenio_eliot_action, worker, invenio_reporters, invenio_records, exitstatus)

@eliotify
def _pytest_sessionfinish(invenio_eliot_action, worker, invenio_reporters, invenio_records, exitstatus):
    """Push the stored patches to redis."""
    # import pytest; pytest.set_trace()
    # Finalize the reporters
    for reporter in invenio_reporters:
        reporter.finalize()

    # Exit if this run was not complete or correct
    if exitstatus != EXIT_OK or worker.retry_after_ids:
        invenio_eliot_action.finish()
        return

    # Upload all collected patches to redis
    idx = 0
    for idx, fullpatch in enumerate(_get_fullpatches_of_last_run('modified', invenio_records, worker), 1):
        Message.log(message_type='adding patch to redis', patch=fullpatch)
        worker.patch_to_redis(fullpatch)
    Message.log(message_type='patch to redis summary', patch_count=idx)

    # End logging. Goodbye.
    invenio_eliot_action.finish()

def _get_fullpatches_of_last_run(step, invenio_records, redis_worker):
    """Return all the record patches resulting from the last run.

    ..note::
        `invenio_records` is populated by the `get_record` function.
    """
    assert step in ('temporary', 'modified')
    for recid, modified_record in invenio_records[step].items():
        original_record = invenio_records['original'][recid]
        patch = jsonpatch.make_patch(original_record, modified_record)
        if patch:
            record_hash = hash(original_record)
            yield make_fullpatch(recid,
                                 record_hash,
                                 patch.to_string(),
                                 redis_worker.task_id)


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
        invenio_records = Session.invenio_records
        worker = Session.config.option.redis_worker
        patches = []
        for fullpatch in _get_fullpatches_of_last_run('temporary', invenio_records, worker):
            del invenio_records['temporary'][fullpatch['recid']]
            patches.append(fullpatch)

        report_exception_ = partial(
            report_exception,
            pytest.config.option.invenio_reporters,  # pylint: disable=no-member
            when, outrep_summary, location_tuple, formatted_exception
        )
        if patches:
            report_exception_(patches=patches)
        else:
            report_exception_()

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

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# pytest_{internalerror,exception_interact}
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def pytest_internalerror(excrepr, excinfo):
    """Called when an internalerror is raised inside pytest."""
    return _pytest_internalerror(excrepr, excinfo)

@eliotify
def _pytest_internalerror(excrepr, excinfo):
    """Log an internal error and terminate logging action."""

    # Eliot does introspection, so we have to HACK around that.
    with mock.patch(
            'eliot._traceback.sys.exc_info',
            mock.Mock(return_value=(excinfo.type, excinfo.value, excinfo.tb))
    ):
        eliot.write_traceback()
    Session.invenio_eliot_action.finish(excinfo.value)

    # Reporters
    finalize_reporters(Session.config.option.invenio_reporters)

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
    _pytest_internalerror(None, call.excinfo)

    exception = call.excinfo.value
    if isinstance(exception, SystemExit):
        redis_worker = node.config.option.redis_worker
        redis_worker.status = StatusWorker.failed

#def pytest_keyboard_interrupt(excinfo):

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# reporter api caller
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
################################################################################

def load_reporters(invenio_rule):
    return [reporter.module.get_reporter(invenio_rule.name)
            for reporter in invenio_rule.reporters]

def report_log(reporters, user_readable_msg, location_tuple):
    # Log to eliot
    Message.log(message_type="check log", msg=user_readable_msg, location=location_tuple)
    # Log to reporters
    if Session.config.option.invenio_execution.should_report_logs:
        for reporter in reporters:
            reporter.report(user_readable_msg, location_tuple)

def report_exception(reporters, when, outrep_summary,
                     location_tuple, formatted_exception):
    for reporter in reporters:
        reporter.report_exception(when, outrep_summary, location_tuple,
                                  formatted_exception)

def finalize_reporters(reporters):
    for reporter in reporters:
        reporter.finalize()
