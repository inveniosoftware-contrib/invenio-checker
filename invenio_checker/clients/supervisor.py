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

"""Load, construct and supervise tasks."""

import os
import math

import jsonpatch
import pytest
from celery import chord, uuid
from invenio_celery import celery
from invenio_base.helpers import with_app_context
from functools import partial

from .master import RedisMaster, StatusMaster
from .worker import RedisWorker, StatusWorker
from eliot import (
    Message,
    to_file,
    start_action,
    start_task,
    Action,
    Logger,
)
from invenio_checker.config import (
    get_eliot_log_file,
    clear_logger_destinations,
)

from _pytest.main import (
    EXIT_OK,
    EXIT_TESTSFAILED,
    EXIT_INTERRUPTED,
    EXIT_INTERNALERROR,
    EXIT_USAGEERROR,
    EXIT_NOTESTSCOLLECTED,
)

from invenio_base.wrappers import lazy_import
from sqlalchemy.orm.exc import NoResultFound

from datetime import datetime
from flask_login import current_user
from invenio.ext.sqlalchemy import db  # pylint: disable=no-name-in-module,import-error
from croniter import croniter
from .redis_helpers import (
    get_lock_partial,
    get_redis_conn,
)

User = lazy_import('invenio_accounts.models.User')
CheckerRule = lazy_import('invenio_checker.models.CheckerRule')
CheckerRuleExecution = lazy_import('invenio_checker.models.CheckerRuleExecution')

this_file_dir = os.path.dirname(os.path.realpath(__file__))
conftest_ini = os.path.join(this_file_dir, '..', 'conftest', 'conftest2.ini')

# from sqlalchemy.orm import sessionmaker
# import sqlalchemy
# from celery.signals import worker_init
# @worker_init.connect
# def initialize_session(sender, signal):
#     """Use a different session when in a worker."""
#     session = sqlalchemy.orm.scoped_session(sessionmaker())
#     session.configure(bind=db.engine)
#     db.session = session
# initialize_session(1, 2)

def _chunks(list_, size):
    """Yield successive size-sized chunks from list_."""
    list_ = list(list_)
    if size == 0:
        yield []
    else:
        for i in xrange(0, len(list_), size):
            yield list_[i:i+size]

def chunk_recids(recids, max_chunks=10, max_chunk_size=1000000):
    """Put given items into chunks that meet given conditions.

    :type max_chunks: int
    :type requested_recids: set
    """
    if not recids:
        return []

    chunks_cnt = int(math.log(len(recids) * 0.01))
    chunks_cnt = max(1, chunks_cnt)

    if chunks_cnt > max_chunks:
        chunks_cnt = max_chunks

    chunk_size = len(recids) / chunks_cnt
    if chunk_size > max_chunk_size:
        chunk_size = max_chunk_size

    return _chunks(recids, chunk_size)

def run_task(task_name, owner=None, dry_run=False):
    # Fail as early as possible if it's missing
    try:
        CheckerRule.query.filter(CheckerRule.name == task_name).one()
    except NoResultFound as e:
        e.args = ('Requested task `{}` not found in the database'
                  .format(task_name),)
        raise

    master_id = uuid()
    # Try to use the currently logged in user as the owner.
    if owner is None:
        cur_user_id = current_user.get_id()
        owner = User.query.get(cur_user_id)
        if owner is None:
            owner = User.query.filter(User.nickname == 'admin').one()

    try:
        new_exec = CheckerRuleExecution(
            uuid=master_id,
            rule_name=task_name,
            start_date=datetime.now(),
            status=StatusMaster.booting,
            owner=owner,
            dry_run=dry_run,
        )
        db.session.add(new_exec)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    _run_task.apply_async(args=(task_name, master_id),
                          task_id=master_id)
    return master_id

@celery.task()
def _run_task(rule_name, master_id):
    # cleanup_failed_runs()

    # redis_master = None
    # def cleanup_session():
    #     # FIXME
    #     if redis_master is not None:
    #         redis_master.zap()

    # def sigint_hook(rcv_signal, frame):
    #     cleanup_session()

    # def except_hook(type_, value, tback):
    #     cleanup_session()
    #     reraise(type_, value, tback)

    # signal.signal(signal.SIGINT, sigint_hook)
    # signal.signal(signal.SIGTERM, sigint_hook)
    # sys.excepthook = except_hook

    from eliot import Logger
    clear_logger_destinations(Logger)
    to_file(get_eliot_log_file(master_id=master_id))
    with start_task(action_type="invenio_checker:supervisor:_run_task",
                    master_id=master_id) as eliot_task:
        eliot_task_id = eliot_task.serialize_task_id()

        # Create master
        Message.log(message_type='creating master')
        redis_master = RedisMaster.create(master_id, eliot_task_id, rule_name)

        # Load rule
        rule = CheckerRule.from_ids((rule_name,)).pop()
        Message.log(message_type='loaded rule', rule_name=rule.name)

        # Ensure test file is present
        if not rule.filepath:
            redis_master.status = StatusMaster.failed
            raise Exception('check file is missing for {}'.format(rule.plugin))

        # Create workers
        subtasks = []
        record_centric = _get_record_fixture_presence(rule.filepath)

        if record_centric:
            if rule.allow_chunking:  # XXX unclear name (run_in_parallel)
                recid_chunks = tuple(chunk_recids(rule.modified_requested_recids))
            else:
                recid_chunks = (rule.modified_requested_recids,)
            Message.log(message_type='creating subtasks', count=len(recid_chunks),
                        mode='record_centric', recid_count=len(rule.modified_requested_recids))
        else:
            recid_chunks = (set(),)
            Message.log(message_type='creating subtasks', count=1,
                        mode='not_record_centric')

        for chunk in recid_chunks:
            task_id = uuid()
            # Must tell the master first so that the worker can detect it
            redis_master.workers_append(task_id)
            subtasks.append(create_celery_task(task_id, redis_master.master_id,
                                               rule, chunk, eliot_task))

        if not subtasks:
            # In record-centric, there's the chance that no records matched
            redis_master.status = StatusMaster.completed
        else:
            redis_master.status = StatusMaster.running
            # FIXME: handle_all_completion should be called after the CALLBACKS!
            callback = handle_all_completion.subtask()
            chord(subtasks)(callback)

def create_celery_task(task_id, master_id, rule, chunk, eliot_task):
    """Return a celery task for a given recid chunk, linked to a master."""
    eliot_task_id = eliot_task.serialize_task_id()
    RedisWorker.create(task_id, eliot_task_id, chunk)
    return run_test.subtask(
        args=(rule.filepath, master_id),
        task_id=task_id,
        link=[handle_worker_completion.s()],
        link_error=[handle_worker_error.s()],
    )

def elioterize(action_type, master_id=None, worker_id=None):
    """Eliot action continuer that can log to either worker or master."""
    clear_logger_destinations(Logger)
    if worker_id:
        to_file(get_eliot_log_file(worker_id=worker_id))
        client = RedisWorker(worker_id)
    elif master_id:
        to_file(get_eliot_log_file(master_id=master_id))
        client = RedisMaster(master_id)

    with Action.continue_task(task_id=client.eliot_task_id):
        return start_action(action_type=action_type)

@celery.task
def handle_worker_completion(task_id):
    """Commit patches.

    :type task_id: str
    :param task_id: task_id as returned by `run_test`
    """
    from invenio_records.api import get_record as get_record_orig
    with elioterize("finalize worker", worker_id=task_id):
        worker = RedisWorker(task_id)
        should_commit = worker.master.get_execution().should_commit
        Message.log(message_type='commit decision', commit=should_commit)

        if should_commit:
            patches_count = 0
            # with db.session.begin_nested():
            for recid, patches in worker.all_patches.items():
                record = get_record_orig(recid)

                first_patch = True
                hash_changed = False
                for patch in patches:

                    if hash_changed:
                        Message.log(message_type='skipping record',
                                    recid=record['id'], worker_id=task_id)
                        break

                    if first_patch:
                        first_patch = False
                        if worker.master.rule.confirm_hash_on_commit:
                            if hash(record) != patch['hash']:
                                hash_changed = True

                    jsonpatch.apply_patch(record, patch, in_place=True)
                    record.commit()
                    patches_count += 1
            Message.log(message_type='committing complete', patches_count=patches_count)
            # FIXME only mark the ones we committed
            worker.master.rule.mark_recids_as_checked(worker.bundle_requested_recids)
            db.session.commit()
        worker.status = StatusWorker.committed

@celery.task
def handle_all_completion(worker_ids):
    # pytest.set_trace()
    master = RedisWorker(worker_ids[0]).master
    with elioterize("handle_task_completion", master_id=master.uuid):
        master.status = StatusMaster.completed

@celery.task
def handle_worker_error(failed_task_id):
    """Handle the fact that a certain task has failed.

    ..note::
        Celery calls this function chunk-times per failed_task_id.

    :param failed_task_id: The UUID of the task that failed.
    :type failed_task_id: str
    """
    # pytest.set_trace()
    with elioterize("handle_worker_error", worker_id=failed_task_id):
        redis_worker = RedisWorker(failed_task_id)
        redis_worker.status = StatusWorker.failed
        redis_worker.master.status = StatusMaster.failed


class CustomRetry(Exception):
    times_retried = 0

    def __init__(self, reason, last_run_still_valid, countdown=None):
        """
        :param countdown: retry countdown in seconds, if needs overriding
        :type countdown: int
        """
        super(CustomRetry, self).__init__(reason)
        self.countdown = countdown
        self.last_run_still_valid = last_run_still_valid
        CustomRetry.times_retried += 1


def _get_record_fixture_presence(check_file):
    retval = pytest.main(
        args=['-s',
              '-p', 'invenio_checker.conftest.conftest_record_fixture_presence',
              '-c', conftest_ini,
              check_file])
    return not retval == EXIT_INTERNALERROR


@celery.task(bind=True, max_retries=None, default_retry_delay=5)
@with_app_context()
def run_test(self, check_file, master_id, retval=None):
    task_id = self.request.id
    redis_worker = RedisWorker(task_id)

    if retval is None:
        redis_worker.status = StatusWorker.booting
        # We set `-c` so that our environment does not affect the test run.
        retval = pytest.main(
            args=[
                '--capture=no',
                '--verbose',
                '--tb=long',
                '-p', 'invenio_checker.conftest.conftest2',  # early-load our plugin
                '-c', conftest_ini,                          # config file
                '--invenio-task-id', task_id,                # custom argument
                check_file]
        )

    # If pytest exited for reason different than a failed check, then something
    # really did break
    exit_success = (EXIT_OK,)
    exit_failure = (EXIT_INTERRUPTED,
                    EXIT_INTERNALERROR,
                    EXIT_USAGEERROR,)
    exit_test_failure = (EXIT_TESTSFAILED,)
    exit_no_tests_collected = (EXIT_NOTESTSCOLLECTED,)

    if retval in exit_test_failure:
        raise Exception("Some tests failed. Not committing to the database.")

    if retval in exit_failure:
        raise Exception("Worker failed with {}.".format(retval))

    # Note that what we do to gracefully terminate pytest when there are
    # blockers is to clear all the collected items. This makes pytest return
    # EXIT_NOTESTSCOLLECTED.
    retry_after_ids = redis_worker.retry_after_ids
    if retval in exit_no_tests_collected:
        if not retry_after_ids:
            raise Exception("No tests collected. Does the specified check exist?")

    # Q: What's with the exception madness here? There's no point.
    # A: Always pass `exc != None` to `self.retry`, because `None` breaks
    # cleanup of workers that are in retry queue (celery/celery#2560)
    try:
        if retry_after_ids:
            raise CustomRetry(
                'Waiting for conflicting checks to finish before running: {}'.\
                format(retry_after_ids), False)
    except CustomRetry as exc:
        if exc.countdown is not None:
            self.retry = partial(self.retry, countdown=exc.countdown)
        if exc.last_run_still_valid:
            self.request.kwargs['retval'] = retval
        else:
            self.request.kwargs['retval'] = self.request.kwargs.pop('retval', None)
        self.retry(exc=exc, kwargs=self.request.kwargs)

    redis_worker.status = StatusWorker.ran
    return task_id


@celery.task
def beat():
    CR = CheckerRule
    with get_lock_partial("invenio_checker:beat_lock", get_redis_conn())():
        scheduled_rules = CR.query.filter(CR.schedule_enabled == True,
                                          CR.schedule != None).all()
        for rule in scheduled_rules:
            iterator = croniter(rule.schedule, rule.last_run)
            next_run = iterator.get_next(datetime)
            now = datetime.now()
            if next_run <= now:
                # Nobody is logged in, so we'll use the owner of the task
                run_task(rule.name, owner=rule.owner)
                rule.last_run = now
                try:
                    db.session.add(rule)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                    raise
