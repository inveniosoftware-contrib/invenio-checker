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

import itertools as it
import signal
import sys
import time
import os

from pytest import main
from celery.exceptions import TimeoutError
from celery import chord, uuid
from six import reraise
from invenio.celery import celery
from invenio.base.helpers import with_app_context

from .redis_helpers import (
    RedisMaster,
    RedisWorker,
    StatusWorker,
    StatusMaster,
    cleanup_failed_runs,
)
from eliot import (
    Message,
    to_file,
    start_action,
    start_task,
    Message,
    Action,
    Logger,
)
from flask import current_app

app = current_app
eliot_log_path = os.path.join(
    app.instance_path,
    app.config.get('CFG_LOGDIR', ''),
    'checker' + '.log.'
)


def _exclusive_paths(path1, path2):
    if not path1.endswith('/'):
        path1 += '/'
    if not path2.endswith('/'):
        path2 += '/'
    return path1.startswith(path2) or path2.startswith(path1)


def _touch_common_paths(paths1, paths2):
    if not paths1:
        paths1 = frozenset({'/'})
    if not paths2:
        paths2 = frozenset({'/'})
    for a, b in it.product(paths1, paths2):
        if _exclusive_paths(a, b):
            return True
    return False


def _touch_common_records(recids1, recids2):
    return recids1 & recids2


def _are_compatible(worker1, worker2):
    return not (_touch_common_records(worker1.allowed_recids, worker2.allowed_recids) and \
        _touch_common_paths(worker1.allowed_paths, worker2.allowed_paths))


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def rules_to_bundles(rules, all_recids):
    max_chunk_size = 1000
    max_chunks = 10
    rule_bundles = {}
    for rule in rules:
        modified_requested_recids = rule.modified_requested_recids
        chunk_size = len(modified_requested_recids)/max_chunks
        if chunk_size > max_chunk_size:
            chunk_size = max_chunk_size
        rule_bundles[rule] = chunks(modified_requested_recids, chunk_size)
    return rule_bundles


def run_task(rule_names):
    for rule_name in rule_names:
        master_id = uuid()
        _run_task.apply_async(args=(rule_name, master_id), task_id=master_id)


@celery.task()
def _run_task(rule_name, master_id):
    del Logger._destinations._destinations[:]
    to_file(open(eliot_log_path + master_id, "ab"))

    with start_task(action_type="invenio_checker:supervisor:_run_task",
                    master_id=master_id) as eliot_task:
        from .models import CheckerRule

        redis_master = None
        group_result = None

        def cleanup_session():
            print 'Cleaning up'
            if redis_master is not None:
                redis_master.zap()

        def sigint_hook(rcv_signal, frame):
            cleanup_session()

        def except_hook(type_, value, tback):
            cleanup_session()
            reraise(type_, value, tback)


        signal.signal(signal.SIGINT, sigint_hook)
        # signal.signal(signal.SIGTERM, sigint_hook)
        sys.excepthook = except_hook

        with start_action(action_type='create master'):
            eliot_task_id = eliot_task.serialize_task_id()
            redis_master = RedisMaster(master_id, eliot_task_id)

        with start_action(action_type='create subtasks'):
            rules = CheckerRule.from_ids((rule_name,))
            bundles = rules_to_bundles(rules, redis_master.all_recids)

            subtasks = []
            errback = handle_error.subtask(args=tuple())
            for rule, rule_chunks in bundles.iteritems():
                for chunk in rule_chunks:
                    task_id = uuid()
                    eliot_task_id = eliot_task.serialize_task_id()
                    RedisWorker(task_id, eliot_task_id, chunk)
                    subtasks.append(run_test.subtask(args=(rule.filepath,
                                                           redis_master.master_id,
                                                           task_id,
                                                           rule.name),
                                                     task_id=task_id,
                                                     link_error=[errback]))

            with start_action(action_type='register subtasks'):
                redis_master.workers = {subtask.id for subtask in subtasks}

        with start_action(action_type='run chord'):
            redis_master.status = StatusMaster.waiting_for_results
            header = subtasks
            callback = handle_results.subtask()
            my_chord = chord(header, track_started=True)
            result = my_chord(callback)


@celery.task
def handle_results(task_ids):
    """Commit patches.

    :type task_ids: list of str
    :param task_ids: values returned by `run_test` instances
    """
    def with_eliot():
        master = RedisWorker(task_ids[0]).master
        master_id = master.uuid
        eliot_task_id = master.eliot_task_id
        del Logger._destinations._destinations[:]
        to_file(open(eliot_log_path + master_id, "ab"))
        with Action.continue_task(task_id=eliot_task_id):
            return start_action(action_type='handle results')

    with with_eliot():
        print task_ids

@celery.task
def handle_error(failed_task_id):
    """
    :type res: list of retvals or list of exception strings
    """
    redis_worker = RedisWorker(failed_task_id)
    redis_worker.status = StatusWorker.terminated

    # TODO (zap things)
    # from celery.contrib import rdb; rdb.set_trace()
    # RedisMaster(failed_master_id).zap()
    # failed_result = AsyncResult(id=failed_task_id)
    # failed_result.maybe_reraise()


@celery.task(bind=True, max_retries=None, default_retry_delay=3)
@with_app_context()
def run_test(self, filepath, master_id, task_id, rule_name):
    redis_worker = RedisWorker(task_id)
    redis_worker.status = StatusWorker.scheduled
    # self.request.retries

    if not redis_worker.retry_after_ids:

        # We set `-c` so that our environment does not affect the test run.
        this_file_dir = os.path.dirname(os.path.realpath(__file__))
        conftest_file = os.path.join(this_file_dir, 'conftest2.ini')
        retval = main(args=['-s', '-v', '--tb=long',
                            '-p', 'invenio_checker.conftest2',
                            '-c', conftest_file,
                            '--invenio-rule', rule_name,
                            '--invenio-task-id', task_id,
                            '--invenio-master-id', master_id,
                            filepath])

    if redis_worker.retry_after_ids:
        # print '{} WAITING ON {}'.format(redis_worker.task_id, redis_worker.retry_after_ids)
        self.retry()
    else:
        redis_worker.status = StatusWorker.terminated
        if retval != 0:
            raise RuntimeError('pytest execution of task {} returned {}'
                               .format(task_id, retval))
        return task_id
