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

from celery import group, uuid
from frozendict import frozendict
from six import reraise

from .redis_helpers import (
    Lock,
    RedisMaster,
    RedisWorker,
    StatusWorker,
    capped_intervalometer,
    get_running_workers,
    StatusMaster,
)
from .tasks import run_test


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = it.tee(iterable)
    next(b, None)
    return it.izip(a, b)


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
    # Dear confused me, this should be a NAND.
    return not (_touch_common_records(worker1['allowed_recids'], worker2['allowed_recids']) and \
        _touch_common_paths(worker1['allowed_paths'], worker2['allowed_paths']))


def _frost_worker(worker, uuid):
    """Freeze workers and put UUID inside."""
    return frozendict({
        'allowed_paths': frozenset(worker['allowed_paths']),
        'allowed_recids': frozenset(worker['allowed_recids']),
        'uuid': uuid,
    })


def split_on_conflict(workers):
    workers = {_frost_worker(data, uuid) for uuid, data in workers.iteritems()}

    # Step 1) All possible compatible combinations
    acceptable_combinations = set()
    for comb in it.combinations(workers, 2):
        for worker1, worker2 in pairwise(comb):
            if _are_compatible(worker1, worker2):
                acceptable_combinations.add(frozenset((worker1, worker2)))

    # Step 2) All possible compatible combinations of length >=1
    acceptable_pairs = set()
    for ok1, ok2 in acceptable_combinations:
        if all(frozenset(combination) in acceptable_combinations
               for combination in it.combinations({ok1}|{ok2}, 2)):
            acceptable_pairs.add(frozenset({ok1}|{ok2}))
    # add workers which didn't match with anything (ones that are now single)
    for worker in workers:
        if not any(worker in acceptable_pair for acceptable_pair in acceptable_pairs):
            acceptable_pairs.add(frozenset({worker}))

    # Step 3) Supersets of length >=1 with no shared elements
    final_pairs = set()
    # For each superset, figure out which contained set already exists in
    # another superset and remove it from the current..
    # ..in reverse sum(id) size order beacause we want to start removing sets
    # from the biggest supersets. While this ordering is not guaranteed to make
    # a least pessimum distribution, it's fast to compute and should yield
    # good enough (R) results.
    gone_through = set()
    for cur_set in sorted(acceptable_pairs,
                          key=lambda s: sum((len(d['allowed_recids']) for d in s)),
                          reverse=True):
        f_exclude = set()
        for f in cur_set:
            # We remove `gone_through` because if we've been through something,
            # we either kept it, or deleted it because it existed somewhere
            # else.
            if {f} & \
            (
                    set(it.chain.from_iterable((acceptable_pairs - set([cur_set]))))
                    - set(it.chain.from_iterable(final_pairs))
                    - gone_through
            ):
                f_exclude.add(f)
        final_pairs.add(cur_set^f_exclude)
        for i in frozenset(it.chain(*set([cur_set]))):  # TODO
            gone_through.add(i)

    # Convert internal representation to uuid-value workers
    transformed_finals = set()
    for tr_group in final_pairs:
        transformed_finals.add((frozenset((fdict['uuid'] for fdict in tr_group))))
    return frozenset(transformed_finals)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def rules_to_bundles(rules):
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
    from .models import CheckerRule

    # TODO: Clean up stray masters by querying them.
    # TODO: Clean up workers that are attached to them
    # XXX: Do we need to lock while doing this?

    redis_master = None
    group_result = None
    my_lock = None
    def cleanup_session(fail=False):
        print 'CLEANUP', fail
        if group_result is not None:
            # Kill workers
            group_result.revoke(terminate=fail)
        if redis_master is not None:
            # Clear master and workers from redis
            redis_master.workers = []
            redis_master.zap()
        if my_lock is not None:
            Lock.release(my_lock)
    def sigint_hook(rcv_signal, frame):
        cleanup_session(fail=True)
        sys.exit(1)
    def except_hook(type_, value, tback):
        cleanup_session(fail=True)
        reraise(type_, value, tback)
    signal.signal(signal.SIGINT, sigint_hook)
    sys.excepthook = except_hook

    # TODO: Split each rule by recids from database filters

    # Load master
    print 'Initializing master'
    redis_master = RedisMaster()

    # Start workers.
    print 'Starting workers'
    rules = CheckerRule.from_ids(rule_names)
    bundles = rules_to_bundles(rules)
    subtasks = []
    for rule, rule_chunks in bundles.iteritems():
        for chunk in rule_chunks:
            task_id = uuid()
            RedisWorker(task_id).bundle_requested_recids = chunk
            subtasks.append(run_test.subtask(args=(rule.filepath, redis_master.master_id, rule.name), task_id=task_id))
    print 'Registering workers'
    redis_master.workers = {subtask.id for subtask in subtasks}

    group_id = uuid()
    job = group((s for s in subtasks), group_id=group_id)
    group_result = job.apply_async()

    # It is important that we keep a list of expected UUIDs so that we know how
    # many answers to anticipate.
    workers = {}
    for worker in redis_master.workers:
        workers[worker] = {}  # id: paths, recids

    # We subscribe to the workers and call the handler every time one of them
    # sends us a message.
    # The handler filles in the values in the `workers` dict. When all the keys
    # in the dict have been filled in, we  know that all the workers have
    # replied.
    def _worker_ready_handler(message):
        task_id = message['channel'].split(':')[2]
        data = message['data']
        if data != 'ready':
            raise ValueError('Bad message from worker: ' + message)
        worker = RedisWorker(task_id)
        workers[worker.task_id] = worker.conflict_dict
    print 'Subscribing to workers'
    redis_master.sub_to_workers((celery_task.id for celery_task in group_result),
                                _worker_ready_handler)
    def on_timeout():
        group_result.revoke(terminate=True)
        # TODO: Clear workers' storage: redis_conn.delete(
        raise Exception('Workers timed out!')

    # FIXME: This waits if there is an exception. Hmm.. Can we check for available workers?
    # Wait for all workers to get ready
    print 'Waiting for workers to get ready'
    capped_intervalometer(float('inf'), 0.5,
                          while_=lambda: not all(workers.values()),
                          do=redis_master.pubsub.get_message,
                          on_timeout=on_timeout)
    redis_master.status = StatusMaster.ready

    # start
    def get_foreign_running_workers(local_workers):
        return {k: v for k, v in  get_running_workers().items() if k not in local_workers}

    def worker_conflicts_with_currently_running(worker):
        group_of_worker = next((w for w in mixed_worker_grouped_names
                                if worker in w))                                                        ; print '!', group_of_worker
        pass                                                                                            ; print '.', foreign_running_workers.keys()
        for gr in foreign_running_workers.keys():
            if gr != group_of_worker:
                print 'TRUE'; return True
        print 'FALSE'; return False

    def resume_test(task_id):
        child = next((child for child in group_result.children
                      if child.task_id == task_id))
        # We know the groups to be mutually exclusive so we wait.
        print 'Starting worker ' + task_id
        ctx_receivers = redis_master.pub_to_worker(task_id, 'run')
        assert ctx_receivers == 1, 'Worker has died ' + task_id
        return child

    # Anything that we finish working with, becomes foreign!

    deleted_workers = set()
    z = set()
    while len(workers) != len(deleted_workers):
        my_lock = Lock.get()
        foreign_running_workers = get_foreign_running_workers(workers)                                  ; print '.', foreign_running_workers
        mixed_worker_grouped_names = list(split_on_conflict(dict(workers, **foreign_running_workers)))  ; print ',', mixed_worker_grouped_names
        for group_ in mixed_worker_grouped_names:
            for worker in workers:
                if worker in group_:
                    if worker not in deleted_workers:
                        if not worker_conflicts_with_currently_running(worker):
                            RedisWorker(worker).status = StatusWorker.running
                            z.add(resume_test(worker))
                            deleted_workers.add(worker)
        Lock.release(my_lock); my_lock = None
        if len(workers) != len(deleted_workers):
            print 'Conflicting workers remain in this group'; time.sleep(1)

    for batman in z:
        print 'Waiting for worker to finish ' + str(batman)
        batman.wait()

    cleanup_session()
