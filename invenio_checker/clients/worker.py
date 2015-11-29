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


import itertools as it

import jsonpatch
from intbitset import intbitset  # pylint: disable=no-name-in-module
from collections import defaultdict
from operator import itemgetter

from .redis_helpers import (
    RedisClient,
    get_workers_in_redis,
    get_redis_conn,
    PleasePylint,
    set_identifier,
    SetterProperty,
)
from invenio_checker.clients.master import master_workers
from invenio_checker.enums import StatusWorker

from invenio_checker.clients.redis_helpers import (
    get_lock_partial,
    prefix_worker,
    client_patches,
    _prefix,
)

# Global
client_examining_lock = _prefix + ':client:examine_lock'

# Worker
worker_allowed_recids = prefix_worker + ':allowed_recids'
worker_allowed_paths = prefix_worker + ':allowed_paths'
worker_requested_recids = prefix_worker + ':requested_recids'

worker_status = prefix_worker + ':status'
worker_retry_after_ids = prefix_worker + ':retry_after_ids'

# References
keys_worker = {
    worker_allowed_recids,
    worker_allowed_paths,
    worker_requested_recids,
    worker_status,
    worker_retry_after_ids,
}

def _workers_touch_common_paths(paths1, paths2):

    def paths_are_exclusive(path1, path2):
        return path1.startswith(path2) or path2.startswith(path1)

    if not paths1:
        paths1 = {'/'}
    if not paths2:
        paths2 = {'/'}
    for a, b in it.product(paths1, paths2):
        if paths_are_exclusive(a, b):
            return True
    return False

def _workers_touch_common_records(recids1, recids2):
    # XXX work around inveniosoftware/intbitset#18
    if any(isinstance(item, type(None)) for item in (recids1, recids2)):
        return False
    return bool(recids1 & recids2)

def workers_are_compatible(worker1, worker2):
    return not (
        _workers_touch_common_records(
            worker1.allowed_recids,
            worker2.allowed_recids)
        and
        _workers_touch_common_paths(
            worker1.allowed_paths,
            worker2.allowed_paths)
    )

def get_workers_with_unprocessed_results():
    """Return all workers in celery  which have started processing, but their
    results have not been handled yet.

    ..note::
        Must be in a lock.
    """
    running_workers = set()
    for worker in get_workers_in_redis():
        if worker.in_celery and worker.status in (StatusWorker.running,
                                                  StatusWorker.ran):
            running_workers.add(worker)
    return running_workers

def make_fullpatch(recid, record_hash, patch, task_id):
    """Create a `fullpatch`-dictionary."""
    return {
        'task_id': task_id,
        'recid': recid,
        'record_hash': record_hash,
        'patch': patch,
    }

def id_to_fullpatch(id_, patch):
    """Generate a `fullpatch`-dictionary."""
    spl = id_.split(':')
    task_id = spl[2]
    recid = int(spl[3])
    record_hash = spl[4]
    return make_fullpatch(recid, record_hash, patch, task_id)

def get_sorted_fullpatches(recid, worker_id):
    """Get sorted fullpatches for this worker.

    :param recid: recid to get patches for
    """
    identifier = client_patches.format(uuid=worker_id, recid=recid,
                                       record_hash='*')
    conn = get_redis_conn()
    for id_ in conn.scan_iter(identifier):
        patch = conn.lpop(id_)
        while patch is not None:
            yield id_to_fullpatch(id_, patch)
            patch = conn.lpop(id_)


class HasAllowedRecids(PleasePylint):

    @property
    @set_identifier(worker_allowed_recids)
    def allowed_recids(self):
        """Get the recids that this worker is allowed to modify."""
        recids = self.conn.get(self._identifier)
        if recids is None:
            return None
        return intbitset(recids)

    @allowed_recids.setter
    @set_identifier(worker_allowed_recids)
    def allowed_recids(self, allowed_recids):
        """Set the recids that this worker will be allowed to modify.

        :type allowed_recids: set
        """
        if allowed_recids is None:
            return self.conn.delete(self._identifier)
        return self.conn.set(self._identifier,
                             intbitset(allowed_recids).fastdump())


class HasAllowedPaths(PleasePylint):

    @property
    @set_identifier(worker_allowed_paths)
    def allowed_paths(self):
        """Get the dictionary paths that this worker is allowed to modify."""
        from redis.exceptions import ResponseError
        try:
            paths = self.conn.get(self._identifier)
            return paths  # can only be None
        except ResponseError:  # WRONGTYPE
            paths = self.conn.smembers(self._identifier)
            return frozenset(paths)

    @allowed_paths.setter
    @set_identifier(worker_allowed_paths)
    def allowed_paths(self, allowed_paths):
        """Set the dictionary paths that this worker will be allowed to modify.

        :type allowed_paths: set
        """
        self.conn.delete(self._identifier)
        if allowed_paths:
            return self.conn.sadd(self._identifier, *allowed_paths)


class HasRetryAfterIds(PleasePylint):
    """Contains IDs of tasks that must exit before we can try again."""

    @property
    @set_identifier(worker_retry_after_ids)
    def retry_after_ids(self):
        """Get task IDs that must finish before we retry running this task."""
        return self.conn.smembers(self._identifier)

    @retry_after_ids.setter
    @set_identifier(worker_retry_after_ids)
    def retry_after_ids(self, worker_ids):
        """Set task IDs that must finish before we retry running this task.

        ..note::
            Must be executed in a lock.

        :type worker_ids: iterable of str
        """
        self.conn.delete(self._identifier)
        if worker_ids:
            return self.conn.sadd(self._identifier, *worker_ids)
        return 0

    @set_identifier(worker_retry_after_ids)
    def retry_after_ids_pop(self, task_id_to_remove):
        """Remove a task upon which the current task depends on.

        :type task_id_to_remove: str
        """
        return self.conn.srem(self._identifier, task_id_to_remove)


class HasStatusWorker(PleasePylint):

    @property
    @set_identifier(worker_status)
    def status(self):
        """Get the status of this worker.

        :rtype: :py:class:`StatusWorker`"
        """
        try:
            return StatusWorker(int(self.conn.get(self._identifier)))
        except TypeError:
            return StatusWorker.dead_parrot

    @status.setter
    @set_identifier(worker_status)
    def status(self, new_status):
        """Set the status of this worker.

        :type new_status: :py:class:`invenio_checker.redis_helpers.StatusWorker`
        """
        assert new_status in StatusWorker
        self.conn.set(self._identifier, new_status.value)

        if new_status == StatusWorker.ran:
            self._on_ran()
        elif new_status == StatusWorker.failed:
            self._on_own_failure()

    def _on_own_failure(self):
        with self.lock():
            self._disappear_as_depender()
            self._clear_own_patches()
            self._cleanup()

    def _on_ran(self):
        with self.lock():
            self._disappear_as_depender()
            self._cleanup()

    def _disappear_as_depender(self):
        """Remove this task from being a dependency of all other tasks."""
        for worker in get_workers_in_redis():
            worker.retry_after_ids_pop(self.task_id)

    def _clear_own_patches(self):
        identifier = client_patches.format(uuid=self.task_id,
                                           recid='*', record_hash='*')
        for redis_patch in self.conn.scan_iter(identifier):
            self.conn.delete(redis_patch)

    def _cleanup(self):
        """Remove this worker from redis."""
        for key in keys_worker:
            self.conn.delete(self.fmt(key))


class HasBundleRequestedRecids(PleasePylint):
    """Contains the record IDs that this worker shall iterate over."""

    @property
    @set_identifier(worker_requested_recids)
    def bundle_requested_recids(self):
        """Get the recids that this worker shall iterate over.

        ..note:: These recids have already been filtered.
        """
        recids = self.conn.get(self._identifier)
        if recids is None:
            recids = set()
        return intbitset(recids)

    @SetterProperty
    @set_identifier(worker_requested_recids)
    def _bundle_requested_recids(self, new_recids):  # pylint: disable=method-hidden
        """Set the recids that this worker shall iterate over.

        :type new_recids: :py:class:`intbitset`
        """
        self.conn.set(self._identifier, intbitset(new_recids).fastdump())


class HasPatches(PleasePylint):
    """Add support for handling fullpatch-style patches in storage."""

    def patch_to_redis(self, fullpatch):
        """
        Called on session finish, once per recid."""
        identifier = client_patches.format(
            uuid=fullpatch['task_id'],
            recid=fullpatch['recid'],
            record_hash=fullpatch['record_hash'],
        )
        self.conn.rpush(identifier, fullpatch['patch'])

    @property
    def all_patches(self):
        identifier = client_patches.format(uuid=self.uuid,
                                           recid='*', record_hash='*')
        patches = defaultdict(list)
        for sub_id in self.conn.scan_iter(identifier):
            for patch in self.conn.lrange(sub_id, 0, -1):
                recid = int(sub_id.split(':')[3])
                patches[recid].append(patch)
        return patches

    def get_record_orig_or_mem(self, recid):
        """Returns record from database, after applying existing patches.

        ..note::
            We upload patches at the end, so we're not picking up our own
            patches here
        """
        # XXX Should we put a lock around this?
        from invenio_records.api import get_record as get_record_orig
        record = get_record_orig(recid)
        sorted_fullpatches = get_sorted_fullpatches(recid, self.uuid)

        for fullpatch in sorted_fullpatches:
            jsonpatch.apply_patch(record, fullpatch['patch'], in_place=True)
        return record


class RedisWorker(RedisClient, HasPatches, HasAllowedRecids, HasRetryAfterIds,
                  HasAllowedPaths, HasStatusWorker, HasBundleRequestedRecids):

    def __init__(self, task_id):
        """Instantiate a handle to all the manifestations of a worker.

        :type task_id: str
        """
        super(RedisWorker, self).__init__(task_id)
        self.lock = get_lock_partial(client_examining_lock, self.conn)

    @classmethod
    def create(cls, task_id, eliot_task_id, bundle_requested_recids):
        """Register this worker in redis and store vital information."""
        new_worker = cls(task_id)
        super(RedisWorker, new_worker).create(eliot_task_id)
        new_worker._bundle_requested_recids = bundle_requested_recids
        new_worker.status = StatusWorker.scheduled
        return new_worker

    @property
    def task_id(self):
        """Legacy name for self.uuid on workers only."""
        return self.uuid

    @property
    def master(self):
        """Get the master of this worker based on redis information."""
        from .master import RedisMaster
        for master in self.conn.scan_iter(master_workers.format(uuid='*')):
            if self.conn.sismember(master, self.task_id):
                master_id = master.split(':')[2]
                return RedisMaster(master_id)
        raise AttributeError("Can't find master!")

    def zap(self):
        """Zap this worker.

        This method is meant to be called from the master.
        """
        # XXX This is silly. Should be done _on_own_failure
        self.status = StatusWorker.failed
        if self.result is not None:
            if not self.result.ready():
                self.result.revoke(terminate=True, signal='TERM')
