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
from _pytest.terminal import TerminalReporter
import traceback
from _pytest.runner import TestReport
import py
from invenio.modules.records.api import get_record
from six import StringIO
from py._io.terminalwriter import TerminalWriter
from contextlib import contextmanager
from .recids import ids_from_input
from .reporter import get_by_name


# Namespace manipulation
# class InvenioStorage(object):
#     def __init__(self):
#         self.records = None
#         self.reporters = None

#     @property
#     def records(self):
#         return pytest.config.

# Helpers
# def pytest_namespace():
#     pass
#     # pytest has special handling for dicts, so we use a custom class instead
#     # invenio_storage = InvenioStorage()
#     # return {'invenio_storage': invenio_storage}


def user_reporters_to_list(reporters):
    # TODO
    # get_reporter(..)
    pass

# Bare fixtures
@pytest.fixture(scope="session")
def db():
    return db

@pytest.fixture(scope="session")
def records():
    return pytest.invenio_storage.records

@pytest.fixture(scope="function")
def log(request):
    def _log(user_readable_msg):
        current_function = request.node  #<class '_pytest.python.Function'>
        import inspect
        frame,filename,line_number,function_name,lines,index = inspect.stack()[1]
        import ipdb; ipdb.set_trace()
        # for reporter in request.config.reporters:
            # reporter.report(user_readable_msg)
    return _log

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
    return
    record_id = request.param
    return get_record(record_id)


# Options
def pytest_addoption(parser):
    # Add env variable. Use `-E whatever` to set
    parser.addoption("--invenio-records", action="store", type=ids_from_input,
        help="set records", dest='invenio_records')
    parser.addoption("--invenio-reporters", action="store", type=user_reporters_to_list,
        help="set reporters", dest='invenio_reporters')

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

################################################################################
################################################################################

# Configure
class InvenioReporter(TerminalReporter):
    def __init__(self, reporter):
        TerminalReporter.__init__(self, reporter.config)

    @contextmanager
    def new_tw(self):
        """Scoped terminal writer to get output of designated functions.

        Every time `getvalue` is called, the manager is ready to run its next
        function.
        """

        def getvalue():
            self._tw._file.seek(0)
            try:
                return self._tw._file.getvalue()
            finally:
                self._tw._file.truncate(0)

        stream = StringIO()
        self._tw = TerminalWriter(file=stream)
        yield getvalue

    def pytest_collectreport(self, report):
        TerminalReporter.pytest_collectreport(self, report)

        # self.rewrite("")  # erase the "collecting" message
        if report.failed:
            self.report_failure(report, when='collect')

    def pytest_runtest_logreport(self, report):
        # TerminalReporter.pytest_runtest_logreport(self, report)  # <-

        if hasattr(report, 'wasxfail'):
            return
        if report.failed:
            self.report_failure(report)

    def pytest_runtest_logstart(self, nodeid, location):
        return  # <-

    def summary_failures(self):
        pass

    def summary_errors(self):
        pass

    def report_failure(self, report, when=None):
        when = when or report.when
        assert when in ('collect', 'setup', 'call', 'teardown')

        with self.new_tw() as getvalue:
            self._outrep_summary(report)
            outrep_summary = getvalue()

        # Output, should use celery?
        fspath, lineno, domain = report.location  # we only care about domain really
        # print ''.join(outrep_summary)
        # print ''.join(traceback.format_exception(report.excinfo.type,
        #                                          report.excinfo.value,
        #                                          report.excinfo.traceback[0]._rawentry))

        # Inform all enabled reporters
        # for reporter in pytest.config.option.invenio_reporters:
        #     reporter.report_exception(report)


@pytest.mark.trylast
def pytest_configure(config):
    """Register our report handlers' handler."""
    if hasattr(config, 'slaveinput'):
        return  # xdist slave, we are already active on the master

    # Get the standard terminal reporter plugin...
    standard_reporter = config.pluginmanager.getplugin('terminalreporter')
    config.pluginmanager.unregister(standard_reporter)

    # Add our own reporter
    invenioreporter = InvenioReporter(standard_reporter)
    config.pluginmanager.register(invenioreporter, 'invenioreporter')

################################################################################
################################################################################

def pytest_runtest_makereport(item, call):
    """Override in order to inject `excinfo`."""
    when = call.when
    duration = call.stop-call.start
    keywords = dict([(x,1) for x in item.keywords])
    excinfo = call.excinfo
    sections = []
    if not call.excinfo:
        outcome = "passed"
        longrepr = None
    else:
        if not isinstance(excinfo, py.code.ExceptionInfo):
            outcome = "failed"
            longrepr = excinfo
        elif excinfo.errisinstance(pytest.skip.Exception):
            outcome = "skipped"
            r = excinfo._getreprcrash()
            longrepr = (str(r.path), r.lineno, r.message)
        else:
            outcome = "failed"
            if call.when == "call":
                longrepr = item.repr_failure(excinfo)
            else: # exception in setup or teardown
                longrepr = item._repr_failure_py(excinfo,
                                            style=item.config.option.tbstyle)
    for rwhen, key, content in item._report_sections:
        sections.append(("Captured std%s %s" %(key, rwhen), content))
    return TestReport(item.nodeid, item.location,
                      keywords, outcome, longrepr, when,
                      sections, duration, excinfo=excinfo)
