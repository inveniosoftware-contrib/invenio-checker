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
import time

from .rules import Rules
from frozendict import frozendict
from functools import partial
from .redis_helpers import capped_intervalometer
from celery import group
from .redis_helpers import RedisMaster
from .redis_helpers import RedisWorker
from uuid import uuid4


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


def _worker_ready_handler(workers, message):
    task_id = message['channel'].split(':')[2]
    data = message['data']
    if data == 'ready':
        worker = RedisWorker(task_id)
        workers[task_id] = {
            'allowed_paths': worker.allowed_paths,
            'allowed_recids': worker.allowed_recids,
        }
    else:
        raise ValueError('Bad message from worker: ' + message)


def run_task(rule_names):
    # TODO: Split each rule
    # common = {
    #     'tickets': tickets,
    #     'queue': queue,
    #     'upload': upload
    # }

    # Load the rules
    rules = Rules.from_ids(rule_names)
    rules = (rules[0], rules[0])   # FIXME: Remove this for production
    a = rules[0].query.requested_ids('ALL')

    # • ▌ ▄ ·.  ▄▄▄· .▄▄ · ▄▄▄▄▄▄▄▄ .▄▄▄
    # ·██ ▐███▪▐█ ▀█ ▐█ ▀. •██  ▀▄.▀·▀▄ █·
    # ▐█ ▌▐▌▐█·▄█▀▀█ ▄▀▀▀█▄ ▐█.▪▐▀▀▪▄▐▀▀▄
    # ██ ██▌▐█▌▐█ ▪▐▌▐█▄▪▐█ ▐█▌·▐█▄▄▌▐█•█▌
    # ▀▀  █▪▀▀▀ ▀  ▀  ▀▀▀▀  ▀▀▀  ▀▀▀ .▀  ▀

    # Unfortunately, it seems that there is no group UUID before apply_async()
    # has finished, so we create our own UUID.
    master_id = str(uuid4())
    redis_master = RedisMaster(master_id)

    # ▄▄▌ ▐ ▄▌      ▄▄▄  ▄ •▄ ▄▄▄ .▄▄▄  .▄▄ ·
    # ██· █▌▐█▪     ▀▄ █·█▌▄▌▪▀▄.▀·▀▄ █·▐█ ▀.
    # ██▪▐█▐▐▌ ▄█▀▄ ▐▀▀▄ ▐▀▀▄·▐▀▀▪▄▐▀▀▄ ▄▀▀▀█▄
    # ▐█▌██▐█▌▐█▌.▐▌▐█•█▌▐█.█▌▐█▄▄▌▐█•█▌▐█▄▪▐█
    #  ▀▀▀▀ ▀▪ ▀█▄▀▪.▀  ▀·▀  ▀ ▀▀▀ .▀  ▀ ▀▀▀▀

    # Start workers.
    from .tasks import run_test
    job = group((
        run_test.s(rule.filepath, master_id)
        for rule in rules
    ))
    group_result = job.apply_async()

    # Make the master aware of its workers
    redis_master.workers = {r.id for r in group_result}

    #  ▄▄ • ▄▄▄ .▄▄▄▄▄     ▄ .▄▪   ▐ ▄ ▄▄▄▄▄.▄▄ ·
    # ▐█ ▀ ▪▀▄.▀·•██      ██▪▐███ •█▌▐█•██  ▐█ ▀.
    # ▄█ ▀█▄▐▀▀▪▄ ▐█.▪    ██▀▐█▐█·▐█▐▐▌ ▐█.▪▄▀▀▀█▄
    # ▐█▄▪▐█▐█▄▄▌ ▐█▌·    ██▌▐▀▐█▌██▐█▌ ▐█▌·▐█▄▪▐█
    # ·▀▀▀▀  ▀▀▀  ▀▀▀     ▀▀▀ ·▀▀▀▀▀ █▪ ▀▀▀  ▀▀▀▀

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
    handler = partial(_worker_ready_handler, workers)
    redis_master.sub_to_workers((celery_task.id for celery_task in group_result),
                                handler)

    # Every time we get a relevant message it is passed to
    # _worker_ready_handler, which in turn fills in the values in `workers`.
    def on_timeout():
        group_result.revoke(terminate=True)
        # TODO: Clear workers' storage: redis_conn.delete(
        raise Exception('Workers timed out!')

    capped_intervalometer(20, 0.01,
                          while_=lambda: not all(workers.values()),
                          do=redis_master.pubsub.get_message,
                          on_timeout=on_timeout)

    # .▄▄ ·  ▄▄▄· ▄▄▄· ▄▄▌ ▐ ▄▌ ▐ ▄      ▄▄ • ▄▄▄        ▄• ▄▌ ▄▄▄·.▄▄ ·
    # ▐█ ▀. ▐█ ▄█▐█ ▀█ ██· █▌▐█•█▌▐█    ▐█ ▀ ▪▀▄ █·▪     █▪██▌▐█ ▄█▐█ ▀.
    # ▄▀▀▀█▄ ██▀·▄█▀▀█ ██▪▐█▐▐▌▐█▐▐▌    ▄█ ▀█▄▐▀▀▄  ▄█▀▄ █▌▐█▌ ██▀·▄▀▀▀█▄
    # ▐█▄▪▐█▐█▪·•▐█ ▪▐▌▐█▌██▐█▌██▐█▌    ▐█▄▪▐█▐█•█▌▐█▌.▐▌▐█▄█▌▐█▪·•▐█▄▪▐█
    #  ▀▀▀▀ .▀    ▀  ▀  ▀▀▀▀ ▀▪▀▀ █▪    ·▀▀▀▀ .▀  ▀ ▀█▄▀▪ ▀▀▀ .▀    ▀▀▀▀

    # start
    worker_groups_names = split_on_conflict(workers)
    for worker_group in worker_groups_names:
        for worker_name in worker_group:
            ctx_receivers = redis_master.pub_to_worker(worker_name, 'run')
            print ctx_receivers  # should be == len(rules)
        time.sleep(4)

    # group_result.ready()  # have all subtasks completed?
    # group_result.successful() # were all subtasks successful?
    # group_result.get()

    # see what the result was(?)
    # for task_id in check_paths:
