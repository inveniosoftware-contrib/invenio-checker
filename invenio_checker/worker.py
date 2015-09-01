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

import jsonpatch
import redis
from enum import Enum
from intbitset import intbitset  # pylint: disable=no-name-in-module
from six import string_types
from warnings import warn
from invenio_records.api import get_record as get_record_orig
from collections import defaultdict
from operator import itemgetter

from .redis_helpers import (
    RedisClient,
    get_workers_in_redis,
    Lock,
    get_redis_conn,
)
from .master import master_workers

_prefix = 'invenio_checker'
_prefix_worker = _prefix + ':worker:{uuid}'
_prefix_master = _prefix + ':master:{uuid}'

# Global
master_examining_lock = _prefix + ':master:examine_lock'
lock_last_owner = _prefix_master + ':examine_lock'

# Worker
worker_allowed_recids = _prefix_worker + ':allowed_recids'
worker_allowed_paths = _prefix_worker + ':allowed_paths'
worker_requested_recids = _prefix_worker + ':requested_recids'
worker_status = _prefix_worker + ':status'
worker_retry_after_ids = _prefix_worker + ':retry_after_ids'
worker_patches_we_applied = _prefix_worker + ':patches_we_applied'  # contains client_patches

# Common
client_eliot_task_id = _prefix + ':{uuid}:eliot_task_id'
client_patches = _prefix + ':patches:{uuid}:{recid}:{record_hash}' # patch

# References
keys_worker = {
    worker_allowed_recids,
    worker_allowed_paths,
    worker_requested_recids,
    worker_status,
    #client_eliot_task_id,
    worker_retry_after_ids,
}

def get_workers_with_unprocessed_results():  # FIXME: Name
    """Return all workers in celery  which have started processing, but their
    results have not been handled yet.

    ..note:: Must be in a lock.
    """
    # conn = get_redis_conn()
    from .worker import StatusWorker
    running_workers = set()
    for worker in get_workers_in_redis():
        if worker.in_celery and worker.status in (StatusWorker.running, StatusWorker.ran):  # XXX Do we want .ran here?
            running_workers.add(worker)
    return running_workers



def fullpatch_to_id(fullpatch):
    return client_patches.format(uuid=fullpatch['task_id'],
                                 recid=fullpatch['recid'],
                                 record_hash=fullpatch['record_hash'])

def make_fullpatch(recid, record_hash, patch, task_id):
    return {
        'recid': recid,
        'record_hash': record_hash,
        'patch': patch,
        'task_id': task_id
    }


def id_to_fullpatch(id_, patch):
    spl = id_.split(':')
    task_id = spl[2]
    recid_ = int(spl[3])
    record_hash = int(spl[4])
    return make_fullpatch(recid_, record_hash, patch, task_id)


def get_fullpatches(recid):
    """Get sorted fullpatches from workers that have not failed.

    :param recid: recid to get patches for
    """

    conn = get_redis_conn()
    identifier = client_patches.format(uuid='*', recid=recid, record_hash='*')
    # FIXME Workers should vanish instead of us filtering here. We should do correct failure propagation.
    # XXX: Should be getting the lock here..?
    patchlists = [
        (conn.ttl(id_), conn.lrange(id_, 0, -1), id_) for id_ in conn.scan_iter(identifier)
        if not RedisWorker(id_.split(':')[2]).a_used_dependency_of_ours_has_failed
        # or RedisWorker(id_.split(':')[2]).status == StatusWorker.failed
    ]

    # sorted_patchets: [[patch_str, patch_str], [patch_str, patch_str]]
    # [set(['[{"path": "/d", "value": "a", "op": "replace"}]'])]

    sorted_fullpatches = []
    from operator import itemgetter
    for ttl, patchlist, id_ in sorted(patchlists, key=itemgetter(0)):
        for patch in patchlist:
            fullpatch = id_to_fullpatch(id_, patch)
            sorted_fullpatches.append(fullpatch)
    return sorted_fullpatches



class StatusWorker(Enum):
    dead_parrot = 0
    scheduled = 1
    booting = 2
    ready = 3
    running = 4
    ran = 5
    failed = 6
    committed = 7


class HasPatches(object):

    @property
    def patches_we_applied(self):
        identifier = worker_patches_we_applied.format(uuid=self.task_id)
        return self.conn.smembers(identifier)

    def patches_we_applied_add(self, fullpatch):
        identifier = worker_patches_we_applied.format(uuid=self.task_id)
        id_ = fullpatch_to_id(fullpatch)
        return self.conn.sadd(identifier, id_)

    def patches_we_applied_clear(self):
        identifier = worker_patches_we_applied.format(uuid=self.task_id)
        for redis_patch in self.conn.scan_iter(identifier):
            self.conn.delete(redis_patch)


class RedisWorker(RedisClient, HasPatches):

    def __init__(self, task_id, eliot_task_id=None, bundle_requested_recids=None):
        """Instantiate a handle to all the manifestations of a worker.

        :type task_id: str
        """
        # TODO: Split to .create()
        # assert (eliot_task_id is not None and bundle_requested_recids is not None)\
        #     or (not eliot_task_id and not bundle_requested_recids)
        initialization = eliot_task_id and bundle_requested_recids

        super(RedisWorker, self).__init__(task_id, eliot_task_id)
        self.lock = Lock(self.task_id)

        if initialization:
            self._bundle_requested_recids = bundle_requested_recids
            self.status = StatusWorker.scheduled

    @property
    def task_id(self):
        return self.uuid

    @property
    def master(self):
        from .master import RedisMaster
        for master in self.conn.scan_iter(master_workers.format(uuid='*')):
            if self.conn.sismember(master, self.task_id):
                master_id = master.split(':')[2]
                return RedisMaster(master_id)
        raise AttributeError("Can't find master!")

    # FIXME
    def zap(self):
        """Zap this worker.

        This method is meant to be called from the master.
        """
        warn('Zapping worker ' + str(self.task_id))

        # self.result.ready()
        if self.result is not None:
            if not self.result.ready():
                self.result.revoke(terminate=True, signal='TERM')
        self._cleanup()

    @property
    def allowed_recids(self):
        """Get the recids that this worker is allowed to modify."""
        identifier = self.fmt(worker_allowed_recids)
        recids = self.conn.get(identifier)
        if recids is None:
            return None
        return intbitset(self.conn.get(identifier))

    @allowed_recids.setter
    def allowed_recids(self, allowed_recids):
        """Set the recids that this worker will be allowed to modify.

        :type allowed_recids: set
        """
        identifier = self.fmt(worker_allowed_recids)
        self.conn.set(identifier, intbitset(allowed_recids).fastdump())

    ############################################################################

    @property
    def retry_after_ids(self):
        """Get task IDs that must finish before we retry running this task."""
        identifier = self.fmt(worker_retry_after_ids)
        return self.conn.smembers(identifier)

    @retry_after_ids.setter
    def retry_after_ids(self, worker_ids):
        """Set task IDs that must finish before we retry running this task.

        ..note::
            Must be executed in a lock.

        :type worker_ids: list of str
        """
        identifier = self.fmt(worker_retry_after_ids)
        self.conn.sadd(identifier, *worker_ids)

    def retry_after_ids_pop(self, task_id_to_remove):
        """Remove a task upon which the current task depends on.

        :type task_id_to_remove: str
        """
        identifier = self.fmt(worker_retry_after_ids)
        self.conn.srem(identifier, task_id_to_remove)

    ############################################################################

    @property
    def allowed_paths(self):
        """Get the dictionary paths that this worker is allowed to modify."""
        identifier = self.fmt(worker_allowed_paths)
        paths = self.conn.smembers(identifier)
        if paths is None:
            return None
        return frozenset(paths)

    @allowed_paths.setter
    def allowed_paths(self, allowed_paths):
        """Set the dictionary paths that this worker will be allowed to modify.

        :type allowed_paths: set
        """
        identifier = self.fmt(worker_allowed_paths)
        self.conn.delete(identifier)
        if allowed_paths:
            self.conn.sadd(identifier, *allowed_paths)

    ############################################################################

    @property
    def status(self):
        """Get the status of this worker.

        :rtype: :py:class:`StatusWorker`"
        """
        identifier = self.fmt(worker_status)
        try:
            return StatusWorker(int(self.conn.get(identifier)))
        except TypeError:
            return StatusWorker.dead_parrot


    @status.setter
    def status(self, new_status):
        """Set the status of this worker.

        :type new_status: :py:class:`invenio_checker.redis_helpers.StatusWorker`
        """
        assert new_status in StatusWorker
        # print 'SETTING STATUS {} ON {}'.format(new_status, self.task_id)

        identifier = self.fmt(worker_status)
        self.conn.set(identifier, new_status.value)

        if new_status == StatusWorker.ran:
            self._on_ran()
        elif new_status == StatusWorker.failed:
            self._on_own_failure()
        elif new_status == StatusWorker.running:
            self.patches_we_applied_clear()

    def _on_own_failure(self):
        self.lock.get()
        try:
            self._disappear_as_depender()
            self._clear_own_patches()
        finally:
            self.lock.release()

    def _on_ran(self):
        self.lock.get()
        try:
            self._disappear_as_depender()
        finally:
            self.lock.release()

    def on_others_failure(self):
        self.lock.get()
        try:
            self._disappear_as_depender()
            self._clear_own_patches()
        finally:
            self.lock.release()

    def _disappear_as_depender(self):
        """Remove this task from being a dependency of all other tasks."""
        for worker in get_workers_in_redis():
            worker.retry_after_ids_pop(self.task_id)

    def _clear_own_patches(self):
        identifier = client_patches.format(uuid=self.task_id, recid='*', record_hash='*')
        for redis_patch in self.conn.scan_iter(identifier):
            self.conn.delete(redis_patch)

    def _cleanup(self):
        """Remove this worker from redis."""
        for key in keys_worker:
            self.conn.delete(self.fmt(key))

    ############################################################################

    @property
    def bundle_requested_recids(self):
        """Get the recids that this worker shall iterate over.

        ..note:: These recids have already been filtered.
        """
        identifier = self.fmt(worker_requested_recids)
        recids = self.conn.get(identifier)
        if recids is None:
            return None
        return intbitset(self.conn.get(identifier))

    @property
    def _bundle_requested_recids(self):
        """Get the recids that this worker shall iterate over."""
        return self.bundle_requested_recids

    @_bundle_requested_recids.setter
    def _bundle_requested_recids(self, new_recids):
        """Set the recids that this worker shall iterate over.

        :type new_recids: :py:class:`intbitset`
        """
        identifier = self.fmt(worker_requested_recids)
        self.conn.set(identifier, intbitset(new_recids).fastdump())

    ############################################################################

    @property
    def task_ids_of_patches_we_worked_ontop(self):
        # Unordered
        remote_ids = set()
        for patch_id in self.patches_we_applied:
            remote_id = patch_id.split(':')[2]
            remote_ids.add(remote_id)
        print 'TASKS FROM WHICH WE USED PATCHES {}'.format(remote_ids)
        return remote_ids

    @property
    def our_used_dependencies_have_committed(self):
        tiopwwo = self.task_ids_of_patches_we_worked_ontop
        for task_id in tiopwwo:
            if RedisWorker(task_id).status != StatusWorker.committed:
                return False
        return True

    @property
    def a_used_dependency_of_ours_has_failed(self):
        # Could speed up XXX
        identifier = client_patches.format(uuid='*', recid='*', record_hash='*')
        patch_ids_in_memory = set()
        for patchlist_id in self.conn.scan_iter(identifier):
            # for id_ in self.conn.smembers(patchlist_id):
            patch_ids_in_memory.add(patchlist_id)
        if self.patches_we_applied <= patch_ids_in_memory:
            return False
        return True

        # tiopwwo = self.task_ids_of_patches_we_worked_ontop
        # for task_id in tiopwwo:
        #     if RedisWorker(task_id).status == StatusWorker.failed:
        #         return True
        # return False

    ############################################################################

    def get_record_orig_or_mem(self, recid):
        # We upload patches at the end, so we're not picking up our own patches here

        # XXX Could put a lock around this but ugh.
        record = get_record_orig(recid)
        record_hash = hash(record)
        sorted_fullpatches = get_fullpatches(recid)

        for fullpatch in sorted_fullpatches:
            # FIXME: Check record hash? Or check in `get_fullpatches`?  see conftest2.py:408
            # assert fullpatch['record_hash'] == record_hash
            self.patches_we_applied_add(fullpatch)
            jsonpatch.apply_patch(record, fullpatch['patch'], in_place=True)
        return record

    def patch_to_redis(self, fullpatch):
        """
        Called on session finish, once per recid."""
        # XXX This should terminate the test
        if self.a_used_dependency_of_ours_has_failed:
            self.on_others_failure()
            return
        conn = get_redis_conn()
        identifier = client_patches.format(
            uuid=fullpatch['task_id'],
            recid=fullpatch['recid'],
            record_hash=fullpatch['record_hash'],
        )
        # assert not conn.get(identifier)
        # print 'patching {} from {}'.format(fullpatch['recid'], fullpatch['task_id'])
        conn.rpush(identifier, fullpatch['patch'].to_string())
        conn.expire(identifier, 365*86400)

    @property
    def all_patches(self):
        identifier = client_patches.format(uuid=self.uuid, recid='*', record_hash='*')
        patches = defaultdict(list)
        for sub_id in self.conn.scan_iter(identifier):
            for patch in self.conn.lrange(sub_id, 0, -1):
                recid = sub_id.split(':')[3]
                patches[recid].append(patch)
        return patches


# client_patches = _prefix + ':patches:{uuid}:{recid}:{record_hash}' # patch

