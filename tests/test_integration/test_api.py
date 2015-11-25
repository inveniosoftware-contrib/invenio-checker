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


"""Integration tests for the checker module API."""

""" Your friendly Q and A:

    Q: Why are you patching `Query`?
    A: Because we never commit to the database, elasticsearch is never
    populated and will never return any results.

    Q: What about nested transactions?
    A: Let's not abuse the session any more than we do in the actual code.

    Q: Some executions report failure, but how do I know why?
    A: `eliot-prettyprint < $(ls -1tr $VIRTUAL_ENV/var/log/checker/* | tail -n1 | head -n1) | less`
"""

import pytest
import sys
import os
from os.path import join as pjoin

from invenio_base.wrappers import lazy_import
from sqlalchemy.orm.exc import NoResultFound
from importlib import import_module

from invenio.testsuite import InvenioTestCase
from intbitset import intbitset  # pylint: disable=no-name-in-module
from mock import patch, MagicMock, Mock, PropertyMock
from invenio_celery import celery
from invenio.ext.sqlalchemy import db  # pylint: disable=no-name-in-module, import-error

CheckerRule = lazy_import('invenio_checker.models.CheckerRule')
CheckerRuleExecution = lazy_import('invenio_checker.models.CheckerRuleExecution')
CheckerReporter = lazy_import('invenio_checker.models.CheckerReporter')

# TODO: test mid-check changing of record hash
# TODO: check beat runs only when it should
# TODO: non-conflicting tests are allowed to run in parallel (non-integration)
# TODO: test stale redis data
# TODO: check performance hints that return bad values
# TODO: def load_task_loads_from_json(self):

CLEANUP_AT_END = False
celery.conf['CELERY_ALWAYS_EAGER'] = True

# These ranges are used to pin-point recids to test with
_start = 5000000
small_rng = range(_start, _start + 4)
large_rng = range(_start, _start + 1000)

# Resolve the location demo check files for patching.
_current_dir = os.path.dirname(__file__)
_tests_dir = os.path.dirname(_current_dir)
_demo_pkg_dir = pjoin(_tests_dir, 'demo_package')
demo_checks_dir = pjoin(_demo_pkg_dir, 'checkerext', 'checks')

# Prepare mocks of check files
filepath_with_class = PropertyMock(return_value=pjoin(demo_checks_dir, 'with_class.py'))
assert os.path.isfile(filepath_with_class())

filepath_without_class = PropertyMock(return_value=pjoin(demo_checks_dir, 'without_class.py'))
assert os.path.isfile(filepath_without_class())

filepath_without_record_fixture = PropertyMock(return_value=pjoin(demo_checks_dir, 'without_record_fixture.py'))
assert os.path.isfile(filepath_without_record_fixture())

filepath_raises_exception_every_time = PropertyMock(return_value=pjoin(demo_checks_dir, 'raises_exception_every_time.py'))
assert os.path.isfile(filepath_raises_exception_every_time())

filepath_two_check_functions = PropertyMock(return_value=pjoin(demo_checks_dir, 'two_check_functions.py'))
assert os.path.isfile(filepath_two_check_functions())

filepath_non_record_centric = PropertyMock(return_value=pjoin(demo_checks_dir, 'non_record_centric.py'))
assert os.path.isfile(filepath_non_record_centric())


def reimport_module(module_name):
    """Import a module to use as a mock without `sys.modules` cache."""
    try:
        del sys.modules[module_name]
    except KeyError:
        pass
    return import_module(module_name)

def get_Query(task_data):
    """Mock for invenio_record.api.Query.

    :param task_data: dictionary
    """
    search_ret = MagicMock(recids=task_data['filter_records'])
    query_ret = MagicMock(search=MagicMock(return_value=search_ret))
    return MagicMock(return_value=query_ret)

def setup_db(username, password):
    """Recreate the database."""
    from invenio_base.scripts.database import drop, create, init

    init(user=username, password=password, yes_i_know=True)
    drop(yes_i_know=True, quiet=False)
    create(quiet=False)

    # Since we are not running `inveniomanage` for this, signals have not been
    # attached, so we populate content manually:

    from invenio_ext.fixtures import load_fixtures
    load_fixtures(None, yes_i_know=True)

    # from invenio_ext.mixer import blend_all
    # blend_all(None, yes_i_know=True)  # Breaks the session.

    # Makre sure something was indeed inserted:
    from invenio_accounts.models import User
    assert User.query.first()

    db.session.flush()
    db.session.expunge_all()
    db.session.commit()


class CheckerTestCase(InvenioTestCase):

    @property
    def config(self):
        config_ = super(CheckerTestCase, self).config
        config_['CFG_DATABASE_NAME'] = 'invenio_checker_integration_tests'
        return config_


class TestApi(CheckerTestCase):
    """Test the API.

    ..note::
        The conftest file in this directory ensures that these tests run in the
        order dictated by the numbering in their names.
    """

    test_entry_prefix = 'INTEGRATION_TEST '

    # TODO: Ensure nothing modifies these:

    # We use this one for most executions.
    task_data = {
        'name': test_entry_prefix + 'Task 1',
        'plugin': 'tests.demo_package.checkerext.checks.with_class',
        'arguments': {'field_name': 'field1', 'new_number_for_field': 3.4},
        'consider_deleted_records': True,
        # 'filter_pattern': 'Higgs',
        'filter_records': intbitset(small_rng),
        'schedule': '30 1 * * *',
        'schedule_enabled': True,
        'force_run_on_unmodified_records': True,
    }

    # This is the one we modify into `edited` (see below).
    task_branch_data = {
        'name': test_entry_prefix + 'Task 1 branch',
        'plugin': 'dummy.dummy_plugin',
        'arguments': {'new': 'arguments'},
        'schedule': '20 1 * * *',
        'schedule_enabled': True,
    }
    task_branched = task_data.copy()
    task_branched.update(task_branch_data)

    # This one is deleted by the deletion checking test.
    task_edit_data = {
        'name': test_entry_prefix + 'Task 1 branch edited',
        'schedule': '10 1 * * *',
    }
    task_edited = task_branched.copy()
    task_edited.update(task_edit_data)

    reporter_data = {
        'plugin': 'tests.demo_package.checkerext.reporters.reporterA',
        'rule_name': task_data['name'],
    }

    def setUp(self):
        reset_reporter()

    def tearDown(self):
        pass

    @pytest.fixture(autouse=True)
    def dbsession(self, request):
        # Roll back at the end of every test
        request.addfinalizer(lambda:db.session.invenio_really_remove())  # pylint: disable=unnecessary-lambda

    @pytest.fixture(autouse=True)
    def load_db_config(self, pytestconfig):

        self.invenio_db_username = pytestconfig.option.invenio_db_username
        self.invenio_db_password = pytestconfig.option.invenio_db_password

    def test_005_setup(self):
        """Clear the database and prevent comitting.

        ..note::
            That this is not in setUp because the application is not ready at
            that point in time.
        """
        setup_db(self.invenio_db_username, self.invenio_db_password)
        # Prevent the session from being closed or committed.
        # Note that these changes occur everywhere during the test run.
        setattr(db.session, 'commit', db.session.flush)
        setattr(db.session, 'invenio_really_remove', db.session.remove)
        setattr(db.session, 'remove', lambda: None)

    def create_records(self, rng):
        """Place records with specific IDs to the database."""
        from invenio_records.api import Record as Rec
        from invenio_records.models import Record, RecordMetadata

        for i in rng:
            rec = Record(id=i)
            db.session.add(rec)
            rec = Rec.create({
                'recid': i,
                'collections': {'primary': 'HEP'},
                'title': 'Record ' + str(i)
            })
            rec.commit()

        db.session.flush()

    def test_017_create_task_creates_task(self):
        from invenio_checker.api import create_task

        # Try to create a task
        new_task = create_task(TestApi.task_data)

        # See if we can get it back out of the database
        task_in_db = CheckerRule.query.filter(CheckerRule.name == TestApi.task_data['name']).one()
        for key, val in TestApi.task_data.iteritems():
            assert getattr(task_in_db, key) == val
        assert task_in_db == new_task

    def test_024_branch_task_branches_task(self):
        from invenio_checker.api import create_task, branch_task
        # Given a task
        create_task(TestApi.task_data)

        # Create a branch
        branched_task = branch_task(TestApi.task_data['name'], TestApi.task_branch_data)

        # Pull it out of the database and check that it's what we asked for
        task_in_db = CheckerRule.query.filter(CheckerRule.name == TestApi.task_branched['name']).one()
        for key, val in TestApi.task_branched.iteritems():
            assert getattr(task_in_db, key) == val
        assert task_in_db == branched_task

    def test_031_edit_task_modifies_task(self):
        from invenio_checker.api import create_task, edit_task

        # Given a task
        create_task(TestApi.task_branched)

        # Edit it
        edited_task = edit_task(TestApi.task_branched['name'], TestApi.task_edit_data)

        # Pull it out of the database and check that it's what we asked for
        task_in_db = CheckerRule.query.filter(CheckerRule.name == TestApi.task_edited['name']).one()
        for key, val in TestApi.task_edited.iteritems():
            assert getattr(task_in_db, key) == val
        assert task_in_db == edited_task

    def test_033_reporter_is_registered(self):
        """Attach a reporter to a task and see if it's actually attached."""
        from invenio_checker.api import create_task, create_reporter
        from invenio_checker.models import get_reporter_db

        # Given a task
        new_task = create_task(TestApi.task_data)

        # Attach a reporter to it
        new_reporter = create_reporter(TestApi.reporter_data)

        # Make sure the reporter was created
        reporter_from_db = get_reporter_db(**TestApi.reporter_data)
        assert reporter_from_db.plugin == TestApi.reporter_data['plugin']
        assert reporter_from_db.rule_name == TestApi.reporter_data['rule_name']

        # and attached to the task
        assert new_task.reporters == [new_reporter]

    def test_034_creating_reporter_demands_presence_of_task(self):
        from invenio_checker.api import create_reporter
        from sqlalchemy.exc import IntegrityError

        # Create a reporter for a task that does not exist
        with pytest.raises(IntegrityError):
            create_reporter(TestApi.reporter_data)

    def test_035_one_reporter_of_each_plugin_allowed_per_task(self):
        """Attach a reporter to a task and see if it's actually attached."""
        from invenio_checker.api import create_task, create_reporter
        from sqlalchemy.orm.exc import FlushError

        # Given a task that has a reporter
        create_task(TestApi.task_data)
        create_reporter(TestApi.reporter_data)

        # Try to attach the same reporter on it
        with pytest.raises(FlushError):
            create_reporter(TestApi.reporter_data)

    def test_036_run_task_with_missing_name_raises(self):
        from invenio_checker.api import run_task

        # Given a task name that's not in the database
        task_name = TestApi.test_entry_prefix + "does not exist"

        # Make sure the task complains before branching into celery
        with pytest.raises(NoResultFound):
            run_task(task_name)

    def test_043_run_task_creates_execution_object_in_database(self):
        from invenio_checker.api import run_task, create_task

        # Given a task
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # Make sure the execution information is stored in the database
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).first()
        assert execution

    # FIRST, we test success
    def test_050_run_reports_successful_completion_with_class(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import run_task, create_task

        # Given a task with a check that has no class
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # Make sure it reports successful completion
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed

    def test_055_run_reports_successful_completion_without_class(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import run_task, create_task

        # Given a task with a check that uses a class
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_without_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # Make sure it reports successful completion
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed

    def test_074_run_modifies_records_appropriately(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that forces re-running on unmodified records
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # Ensure that it modifies the records as coded
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        for i in small_rng:
            assert get_record(i)['field1'] == 3.4

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # And that it picks up the changes it did last time on the next run
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        for i in small_rng:
            assert get_record(i)['field1'] == 3.4 * 2

    # TODO: test success when no records are found in the query
    #def test_074_run_reports_sucess_when_no_records_found(self):

    def test_075_run_with_dry_run_does_not_modify_records(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that forces re-running on unmodified records
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'], dry_run=True)

        # Ensure that it modifies the records as coded
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        for i in small_rng:
            assert 'field1' not in get_record(i)

    def test_076_run_with_dry_run_does_not_call_reporters_for_logs(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that forces re-running on unmodified records
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'], dry_run=True)

        # Ensure that it modifies the records as coded
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        assert reported_reports == 0

    def test_077_run_with_dry_run_does_not_call_reporters_for_exceptions(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that forces re-running on unmodified records
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_raises_exception_every_time):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'], dry_run=True)

        # Ensure that it modifies the records as coded
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.failed
        assert reported_exceptions == 0

    # @pytest.skip('Nesting pytest is apparently a bad idea.')
    def test_077_run_on_non_record_centric_calls_check_function(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that does not force re-running on unmodified records
        task_data = dict(TestApi.task_data)
        task_data['force_run_on_unmodified_records'] = False
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records([6000000, 6000001])

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_non_record_centric):
            with patch('invenio_checker.models.Query', Query):
                run_task(task_data['name'])
                task_id = run_task(task_data['name'])

        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        assert get_record(6000000)['a_field'] == 'a_value_2'
        assert get_record(6000001)['a_field'] == 'another_value'

    def test_0100_run_does_not_run_twice_on_records_that_did_no_change_since_last_run(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task that does not force re-running on unmodified records
        task_data = dict(TestApi.task_data)
        task_data['force_run_on_unmodified_records'] = False
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                run_task(task_data['name'])
                task_id = run_task(task_data['name'])

        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        for i in small_rng:
            assert get_record(i)['field1'] == 3.4

    def test_0101_run_without_record_fixture_runs_once(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task
        from invenio_records.api import get_record

        # Given a task
        task_data = TestApi.task_data
        # Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_without_record_fixture):
            task_id = run_task(task_data['name'])

        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        assert get_record(small_rng[0])['this_record_has_id_1'] == True
        assert 'this_record_has_id_1' not in get_record(small_rng[1])

    def test_0102_worker_that_spun_returns_result(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task

        # Given a task
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        # ..that conflicts with other workers for a while
        conflicts = (
            {MagicMock(uuid=1), MagicMock(uuid=2)},
            {MagicMock(uuid=1)},
            {},
        )

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_without_class):
            with patch('invenio_checker.models.Query', Query):
                with patch('invenio_checker.conftest.conftest2._worker_conflicts_with_currently_running', side_effect=conflicts):
                    task_id = run_task(task_data['name'])

        # Ensure it finishes successfully
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed

    def test_0103_run_initialized_reporters_only_when_not_spinning(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import create_task, run_task, create_reporter

        # Given a task with a reporter
        task_data = TestApi.task_data
        new_task = create_task(task_data)
        new_reporter = create_reporter(TestApi.reporter_data)
        Query = get_Query(task_data)
        self.create_records(small_rng)

        # ..while tracking the reporter's initialization
        reporterA = reimport_module('tests.demo_package.checkerext.reporters.reporterA')
        reporterA.get_reporter = MagicMock()

        # ..as well as calls to the task conflict resolver
        conflict_resolver = Mock(side_effect=({MagicMock(uuid=1)}, {}))
        mock_manager = Mock()
        mock_manager.attach_mock(reporterA.get_reporter, 'get_reporter')
        mock_manager.attach_mock(conflict_resolver, 'conflict_resolver')

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_without_class):
            with patch('invenio_checker.models.Query', Query):
                with patch('invenio_checker.models.CheckerReporter.module', reporterA):
                    with patch('invenio_checker.conftest.conftest2._worker_conflicts_with_currently_running',
                               conflict_resolver):
                        task_id = run_task(task_data['name'])

        # (Better safe than (very) sorry)
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed

        # Ensure that the reporter was not initialized before no conflicts were remaining
        from ..conftest import contains_sublist
        assert contains_sublist(
            [call[0] for call in mock_manager.mock_calls],  # func names
            [
                'conflict_resolver',
                'conflict_resolver',
                'get_reporter',
            ]
        )

    # AFTERWARDS, we test failure
    def test_0106_run_fails_when_file_is_missing(self):
        """
        ..note::
            It shouldn't be necessary to have records for this to fail but
            that's how it is. FIXME
        """
        from invenio_checker.api import create_task, run_task
        from invenio_checker.clients.master import StatusMaster

        # Given a task..
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        # ..whose check file is absent
        with patch('invenio_checker.models.CheckerRule.filepath', None):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # TODO Use this elsewhere too?
        try:
            Query.assert_called_once_with(task_data['filter_pattern'])
        except KeyError:
            pass

        # Ensure that it failed
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.failed

    def test_0107_run_calls_reporters_when_there_is_exception(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import run_task, create_task, create_reporter

        # Given a task..
        task_data = TestApi.task_data
        new_task = create_task(task_data)
        # ..with a reporter attached
        new_reporter = create_reporter(TestApi.reporter_data)
        # and some records in the database
        self.create_records(small_rng)

        reporterA = reimport_module('tests.demo_package.checkerext.reporters.reporterA')
        Query = get_Query(task_data)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_raises_exception_every_time):
            with patch('invenio_checker.models.Query', Query):
                with patch('invenio_checker.models.CheckerReporter.module', reporterA):
                    task_id = run_task(task_data['name'])

        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.failed
        assert reported_exceptions == len(small_rng)

    def test_0110_run_calls_reporters_when_check_wants_to_log(self):
        from invenio_checker.clients.master import StatusMaster
        from invenio_checker.api import run_task, create_task, create_reporter

        # Given a task..
        task_data = TestApi.task_data
        new_task = create_task(task_data)
        # ..with a reporter attached
        new_reporter = create_reporter(TestApi.reporter_data)
        # and some records in the database
        self.create_records(small_rng)

        reporterA = reimport_module('tests.demo_package.checkerext.reporters.reporterA')
        Query = get_Query(task_data)

        with patch('invenio_checker.models.CheckerRule.filepath', filepath_with_class):
            with patch('invenio_checker.models.Query', Query):
                with patch('invenio_checker.models.CheckerReporter.module', reporterA):
                    task_id = run_task(task_data['name'])

        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.completed
        assert reported_reports == len(small_rng)

    def test_0110_run_with_multiple_functions_in_check_file_fails(self):
        from invenio_checker.api import create_task, run_task
        from invenio_checker.clients.master import StatusMaster

        # Given a task..
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        create_task(task_data)
        self.create_records(small_rng)

        # ..where the file has too many check functions
        with patch('invenio_checker.models.CheckerRule.filepath', filepath_two_check_functions):
            with patch('invenio_checker.models.Query', Query):
                task_id = run_task(task_data['name'])

        # Ensure that it failed
        execution = CheckerRuleExecution.query.filter(CheckerRuleExecution.uuid == task_id).one()
        assert execution.status == StatusMaster.failed

    def test_0113_delete_task_deletes_task(self):
        from invenio_checker.api import delete_task, create_task

        # Given a task
        task_data = TestApi.task_edited
        create_task(task_data)

        # Delete the task
        delete_task(task_data['name'])

        # Make sure it cannot be found anymore
        with pytest.raises(NoResultFound):
            CheckerRule.query.filter(CheckerRule.name == TestApi.task_edited['name']).one()

    def test_0115_delete_task_with_reporter_deletes_task_and_reporter(self):
        from invenio_checker.api import create_task, create_reporter
        from invenio_checker.api import delete_task

        # Given a task..
        task_data = TestApi.task_data
        Query = get_Query(task_data)
        new_task = create_task(task_data)
        self.create_records(small_rng)

        # ..with a reporter attached
        new_reporter = create_reporter(TestApi.reporter_data)

        # Delete the task
        delete_task(task_data['name'])

        # Assert the reporter is gone as well
        assert CheckerReporter.query.filter(CheckerReporter.rule_name == task_data['name']).first() == None

    def test_0120_deleting_non_existing_task_raises(self):
        from invenio_checker.api import delete_task

        # Ensure trying to delete a missing task raises
        with pytest.raises(NoResultFound):
            delete_task(TestApi.test_entry_prefix + 'THIS DOESNT EXIST')

    def test_05000_cleanup(self):
        if CLEANUP_AT_END:
            self.test_005_setup()


reported_reports = 0
reported_exceptions = 0

def reset_reporter():
    global reported_reports
    global reported_exceptions
    reported_reports = 0
    reported_exceptions = 0


class ReporterA(object):
    """
    ..note::
        This reporter is located here so that we can check how its functions
        are called.
    """

    def __init__(self, db_entry, execution):
        self.db_entry = db_entry
        self.execution = execution

    def report_exception(self, when, outrep_summary, location_tuple, formatted_exception=None, patches=None):
        global reported_exceptions
        reported_exceptions += 1

    def report(self, user_readable_msg, location_tuple=None):
        global reported_reports
        reported_reports += 1

    def finalize(self):
        pass

from invenio.testsuite import make_test_suite, run_test_suite
TEST_SUITE = make_test_suite(TestApi)
if __name__ == "__main__":
    run_test_suite(TEST_SUITE)
    # nest.commit()
