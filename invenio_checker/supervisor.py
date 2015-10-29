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

import signal
import sys
import time
import os
import math

import jsonpatch
from pytest import main
from celery import chord, uuid
from six import reraise
from invenio.celery import celery
from invenio.base.helpers import with_app_context
from functools import partial

from .master import RedisMaster, StatusMaster
from .worker import RedisWorker, StatusWorker
from .redis_helpers import cleanup_failed_runs
from eliot import (
    Message,
    to_file,
    start_action,
    start_task,
    Action,
    Logger,
)
from .config import get_eliot_log_path
eliot_log_path = get_eliot_log_path()

from _pytest.main import (
    EXIT_OK,
    EXIT_TESTSFAILED,
    EXIT_INTERRUPTED,
    EXIT_INTERNALERROR,
    EXIT_USAGEERROR,
)

from invenio_base.wrappers import lazy_import

from datetime import datetime
from flask_login import current_user
from invenio.ext.sqlalchemy import db
from croniter import croniter
from .redis_helpers import (
    get_lock_partial,
    get_redis_conn,
)

User = lazy_import('invenio_accounts.models.User')
CheckerRule = lazy_import('invenio_checker.models.CheckerRule')
CheckerRuleExecution = lazy_import('invenio_checker.models.CheckerRuleExecution')


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

def run_task(task_name):
    master_id = uuid()
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
        )
        db.session.add(new_exec)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise

    _run_task.apply_async(args=(task_name, master_id),
                          task_id=master_id)

@celery.task()
def _run_task(rule_name, master_id):
    # cleanup_failed_runs()

    redis_master = None
    def cleanup_session():
        # FIXME
        if redis_master is not None:
            redis_master.zap()

    def sigint_hook(rcv_signal, frame):
        cleanup_session()

    def except_hook(type_, value, tback):
        cleanup_session()
        reraise(type_, value, tback)

    signal.signal(signal.SIGINT, sigint_hook)
    signal.signal(signal.SIGTERM, sigint_hook)
    sys.excepthook = except_hook

    del Logger._destinations._destinations[:] # prevent dupes in logger
    to_file(open(os.path.join(eliot_log_path, master_id), "ab"))
    with start_task(action_type="invenio_checker:supervisor:_run_task",
                    master_id=master_id) as eliot_task:
        eliot_task_id = eliot_task.serialize_task_id()
        Message.log(message_type='creating master')
        redis_master = RedisMaster.create(master_id, eliot_task_id,
                                          rule_name)

        rule = CheckerRule.from_ids((rule_name,)).pop()
        bundles = list(chunk_recids(rule.modified_requested_recids))
        Message.log(message_type='creating {} subtasks'.format(len(bundles)))
        subtasks = []
        for id_chunk in bundles:
            task_id = uuid()
            # We need to tell the master first so that the worker knows
            redis_master.workers_append(task_id)
            eliot_task_id = eliot_task.serialize_task_id()
            RedisWorker.create(task_id, eliot_task_id, id_chunk)
            subtasks.append(
                run_test.subtask(
                    args=(rule.filepath, redis_master.master_id),
                    task_id=task_id,
                    link=[handle_worker_completion.s()],
                    link_error=[handle_worker_error.s()],
                )
            )

        redis_master.status = StatusMaster.running
        callback = handle_all_completion.subtask(
            # link_error=[handle_master_error.s(redis_master.master_id)]
        )
        result = chord(subtasks)(callback)

def with_eliot(action_type, master_id=None, worker_id=None):
    assert master_id or worker_id
    if worker_id:
        # print "WITH {}".format(worker_id)
        master_id = RedisWorker(worker_id).master.master_id
    master = RedisMaster(master_id)
    eliot_task_id = master.eliot_task_id
    del Logger._destinations._destinations[:]
    to_file(open(eliot_log_path + master_id, "ab"))
    with Action.continue_task(task_id=eliot_task_id):
        return start_action(action_type=action_type)

@celery.task
def handle_worker_completion(task_id):
    """Commit patches.

    :type task_id: str
    :param task_id: task_id as returned by `run_test`
    """
    from invenio_records.api import get_record as get_record_orig
    redis_worker = RedisWorker(task_id)
    for recid, patches in redis_worker.all_patches.items():
        record = get_record_orig(recid)
        for patch in patches:
            jsonpatch.apply_patch(record, patch, in_place=True)
        record.commit()
    redis_worker.master.rule.mark_recids_as_checked(redis_worker.bundle_requested_recids)
    redis_worker.status = StatusWorker.committed

@celery.task
def handle_all_completion(worker_ids):
    master = RedisWorker(worker_ids[0]).master
    master.status = StatusMaster.completed

@celery.task
def handle_worker_error(failed_task_id):
    """Handle the fact that a certain task has failed.

    ..note::
        Celery calls this function chunk-times per failed_task_id.

    :param failed_task_id: The UUID of the task that failed.
    :type failed_task_id: str
    """
    redis_worker = RedisWorker(failed_task_id)
    redis_worker.status = StatusWorker.failed
    redis_worker.master.status = StatusMaster.failed


class CustomRetry(Exception):
    def __init__(self, reason, last_run_still_valid, countdown=None):
        """
        :param countdown: retry countdown in seconds, if needs overriding
        :type countdown: int
        """
        super(CustomRetry, self).__init__(reason)
        self.countdown = countdown
        self.last_run_still_valid = last_run_still_valid


@celery.task(bind=True, max_retries=None, default_retry_delay=5)
@with_app_context()
def run_test(self, check_file, master_id, retval=None):
    task_id = self.request.id
    redis_worker = RedisWorker(task_id)
    print 'ENTER {} of GROUP {}'.format(task_id, redis_worker.master.uuid)

    if retval is None:
        redis_worker.status = StatusWorker.booting
        # We set `-c` so that our environment does not affect the test run.
        this_file_dir = os.path.dirname(os.path.realpath(__file__))
        conftest_file = os.path.join(this_file_dir, 'conftest2.ini')
        print 'RUNNING {} of {}'.format(task_id, redis_worker.master.uuid)
        retval = main(args=['-s', '-v', '--tb=long',
                            '-p', 'invenio_checker.conftest2',
                            '-c', conftest_file,
                            '--invenio-task-id', task_id,
                            '--invenio-master-id', master_id,
                            check_file])

    # If pytest exited for reason different than a failed check, then something
    # really did break
    exit_success = (EXIT_OK,)
    exit_failure = (EXIT_INTERRUPTED,
                    EXIT_INTERNALERROR,
                    EXIT_USAGEERROR,)
    exit_test_failure = (EXIT_TESTSFAILED,)

    if retval in exit_test_failure:
        raise Exception("Some tests failed. Not committing to the database.")

    if retval in exit_failure:
        raise Exception("Worker failed with {}.".format(retval))

    # Always pass `exc != None` to `self.retry`, because `None` breaks cleanup
    # of workers that are in retry queue (celery/celery#2560)
    try:
        retry_after_ids = redis_worker.retry_after_ids
        if retry_after_ids:
            raise CustomRetry(
                'Waiting for conflicting checks to finish before running: {}'.\
                format(retry_after_ids), False)
        # We are looking to commit below this line
        redis_worker.status = StatusWorker.ran
    except CustomRetry as exc:
        if exc.countdown is not None:
            self.retry = partial(self.retry, countdown=exc.countdown)
        if exc.last_run_still_valid:
            self.request.kwargs.update(retval=retval)
        else:
            self.request.kwargs.pop('retval', None)
        self.retry(exc=exc, kwargs=self.request.kwargs)

    print 'RETURNING {} {}'.format(task_id, master_id)
    return task_id


@celery.task
def beat():
    CR = CheckerRule
    with get_lock_partial("invenio_checker:beat_lock", get_redis_conn())():
        scheduled_rules = CR.query.filter(CR.schedule_enabled == True).all()
        for rule in scheduled_rules:
            iterator = croniter(rule.schedule, rule.last_run)
            next_run = iterator.get_next(datetime)
            now = datetime.now()
            if next_run <= now:
                run_task(rule.name)
                rule.last_run = now
                try:
                    db.session.add(rule)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                    raise
