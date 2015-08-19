import time

import jsonpatch
import sys
import redis
import redlock
from celery import states
from celery.result import AsyncResult
from celery.task.control import inspect  # pylint: disable=no-name-in-module, import-error
from enum import Enum
from intbitset import intbitset  # pylint: disable=no-name-in-module
from six import string_types
from warnings import warn
import signal
from invenio_records.api import get_record as get_record_orig


_prefix = 'invenio_checker'
_prefix_worker = _prefix + ':worker:{uuid}'
_prefix_master = _prefix + ':master:{uuid}'

# Global
master_examining_lock = _prefix + ':master:examine_lock'
lock_last_owner = _prefix_master + ':examine_lock'

# Master
master_workers = _prefix_master + ':workers'
master_all_recids = _prefix_master + ':all_recids'
master_status = _prefix_master + ':status'

# Worker
worker_allowed_recids = _prefix_worker + ':allowed_recids'
worker_allowed_paths = _prefix_worker + ':allowed_paths'
worker_requested_recids = _prefix_worker + ':requested_recids'
worker_status = _prefix_worker + ':status'
worker_retry_after_ids = _prefix_worker + ':retry_after_ids'

# Common
client_eliot_task_id = _prefix + ':{uuid}:eliot_task_id'
client_patches = _prefix + ':patches:{uuid}:{recid}:{record_hash}' # patch

# References
keys_master = {
    master_workers,
    master_all_recids,
    master_status,
    client_eliot_task_id
}
keys_worker = {
    worker_allowed_recids,
    worker_allowed_paths,
    worker_requested_recids,
    worker_status,
    client_eliot_task_id,
    worker_retry_after_ids,
}


# config['CACHE_REDIS_URL']  # FIXME
redis_uri_string = 'redis://localhost:6379/1'


# class RedisConn(object):
#     redis_conn = redis.StrictRedis.from_url(redis_uri_string)


def _get_redis_conn():
    # return RedisConn.redis_conn
    return redis.StrictRedis.from_url(redis_uri_string)


def _get_all_masters(conn):
    """Return all masters found in redis.

    :type conn: :py:class:`redis.client.StrictRedis`
    """
    return set([RedisMaster(master_worker.split(':')[2])
                for master_worker in conn.scan_iter(master_workers.format(uuid='*'))])


def _get_lock_manager():
    """Get an instance of redlock for the configured redis servers."""
    return redlock.Redlock([redis_uri_string])


def get_workers_with_unprocessed_results():
    """Return all workers which have started processing, but their results have
    not been handled yet.

    ..note:: Must be in a lock.
    """
    # cleanup_failed_runs()
    conn = _get_redis_conn()
    masters = _get_all_masters(conn)
    running_workers = set()
    for master in masters:
        for worker in [RedisWorker(worker) for worker in master.workers]:
            if worker.status == StatusWorker.running:
                running_workers.add(worker)
    return running_workers


def _get_things_in_redis(prefixes, **kwargs):
    """Return the IDs of all masters or workers in redis.

    :type prefixes: set

    :rtype: set
    """
    if not kwargs:
        kwargs = {'uuid': '*'}
    conn = _get_redis_conn()
    ids = set()
    for pref in prefixes:
        for field in conn.scan_iter(pref.format(**kwargs)):
            id_ = field.split(':')[2]
            ids.add(id_)
    return ids


def get_masters_in_redis():
    return set([RedisMaster(master_id) for master_id in _get_things_in_redis(keys_master)])


def get_workers_in_redis():
    return set([RedisWorker(worker_id) for worker_id in _get_things_in_redis(keys_worker)])


def cleanup_failed_runs():
    slept = False
    for master in get_masters_in_redis():
        if not master.in_celery:
            if not slept:
                time.sleep(2)
                slept = True
            master.zap()
    # We probably don't want to remove results from celery!
    for worker in get_workers_in_redis():
        if not worker.in_celery:
            if not slept:
                time.sleep(2)
                slept = True
            worker.zap()


class Lock(object):

    def __init__(self, master_id):
        """Initialize a lock handle for a certain master.

        :type master_id: str
        """
        self.conn = _get_redis_conn()
        self.master_id = master_id
        self._lock_manager = _get_lock_manager()

    @property
    def _lock(self):
        """Redis representation of the lock object."""
        ret = self.conn.hgetall(lock_last_owner)
        return {
            'last_master_id': ret['last_master_id'],
            'tuple': redlock.Lock(
                validity=ret['lock_validity'],
                resource=ret['lock_resource'],
                key=ret['lock_key']
            )
        }

    @_lock.setter
    def _lock(self, tuple_):
        """Redis representation of the lock object.

        :type tuple_: :py:class:`redlock.Lock`
        """
        self.conn.hmset(
            lock_last_owner,
            {
                'last_master_id': self.master_id,
                'lock_validity': tuple_.validity,
                'lock_resource': tuple_.resource,
                'lock_key': tuple_.key
            }
        )

    def get(self):
        """Block while trying to claim the lock to this master."""
        while True:
            new_lock = self._lock_manager.lock(master_examining_lock, 1000)
            if new_lock:
                assert self.conn.persist(master_examining_lock)
                self._lock = new_lock
                # print 'GOT LOCK!'
                break
            else:
                print 'Failed to get lock, trying again.'
                time.sleep(0.5)

    def release(self):
        """Release the lock if it belongs to this master."""
        if self._lock['last_master_id'] != self.master_id:
            # print 'NOT MY LOCK'
            return
        # print 'RELEASING LOCK!'
        self._lock_manager.unlock(self._lock['tuple'])


class RedisClient(object):
    def __init__(self, uuid, eliot_task_id=None):
        """Initialize a lock handle for a certain client."""
        self.conn = _get_redis_conn()
        self.uuid = uuid
        if eliot_task_id is not None:
            self.eliot_task_id = eliot_task_id

    @property
    def eliot_task_id(self):
        identifier = self.fmt(client_eliot_task_id)
        return self.conn.get(identifier)

    @eliot_task_id.setter
    def eliot_task_id(self, eliot_task_id):
        identifier = self.fmt(client_eliot_task_id)
        return self.conn.set(identifier, eliot_task_id)

    def fmt(self, string):
        """Format a redis string with the current master_id.

        :type string: str
        """
        return string.format(uuid=self.uuid)

    @property
    def result(self):
        """Get the celery result object.

        ..note:: This will return even if the object does not exist.

        :rtype: :py:class:`celery.result.AsyncResult`
        """
        return AsyncResult(id=self.uuid)

    @property
    def in_celery(self):
        """Return whether this client exists as a process in celery."""
        insp = inspect()
        active = insp.active()
        for active_tasks in active.values():
            for active_task in active_tasks:
                if active_task['id'] == self.uuid:
                    return True
        return False

    def __eq__(self, other):
        """Compare with other clients in a rich manner.

        :type other: str
        """
        if isinstance(other, string_types):
            return self.uuid == other
        return self.uuid == other.uuid

    def __ne__(self, other):
        """Compare with other clients in a rich manner.

        :type other: str
        """
        if isinstance(other, string_types):
            return self.uuid == other
        return self.uuid != other.uuid

    def __hash__(self):
        return hash(self.uuid)


class StatusMaster(Enum):
    booting = 1
    waiting_for_results = 2


class RedisMaster(RedisClient):
    def __init__(self, master_id, eliot_task_id=None):
        """Initialize a lock handle for a certain master.

        ..note::
            Registers the master with redis if it does not already exist.

        :type master_id: str
        """
        from invenio_records.models import Record
        super(RedisMaster, self).__init__(master_id, eliot_task_id)

        if not RedisMaster._already_instnatiated(master_id):
            self.all_recids = Record.allids()  # __init__ is a good time for this.
            self.status = StatusMaster.booting  # TODO: Not useful?

    @property
    def master_id(self):
        return self.uuid

    @staticmethod
    def _already_instnatiated(master_id):
        """Return whether this master is already registered with redis.

        :type master_id: str
        """
        conn = _get_redis_conn()
        for prefix in keys_master:
            for field in conn.scan_iter(prefix.format(uuid='*')):
                id_ = field.split(':')[2]
                if id_ == master_id:
                    return True
        return False

    def zap(self):
        """Zap this master and its workers.

        This method is meant to be called by a SIGINT or exception handler from
        within the master process itself.
        """
        # FIXME: Should we lock here?
        signal.signal(signal.SIGINT, lambda rcv_signal, frame: None)
        # Kill workers
        for redis_worker in self.redis_workers:
            redis_worker.zap()
        warn('Zapping master ' + str(self.master_id))
        self._cleanup()

    def _cleanup(self):
        """Clear this master from redis."""
        for key in keys_master:
            self.conn.delete(self.fmt(key))
        # Release lock
        self.lock.release()

    @property
    def workers(self):
        """Get all the worker IDs associated with this master.

        :rtype: set of :py:class:`WorkerRedis`
        """
        identifier = self.fmt(master_workers)
        return self.conn.smembers(identifier)

    @workers.setter
    def workers(self, worker_ids):
        """Associate workers with this master.

        :type worker_ids: set of IDs
        """
        identifier = self.fmt(master_workers)
        self.conn.delete(identifier)
        if worker_ids:
            self.conn.sadd(identifier, *worker_ids)

    @property
    def redis_workers(self):
        """Get all the workers associated with this master.

        :rtype: set of :py:class:`WorkerRedis`
        """
        return {RedisWorker(worker_id) for worker_id in self.workers}

    @property
    def all_recids(self):
        """Get all recids that are assumed to exist by tasks of this master."""
        identifier = self.fmt(master_all_recids)
        recids_set = self.conn.get(identifier)
        if recids_set is None:
            return None
        return intbitset(recids_set)

    @all_recids.setter
    def all_recids(self, recids):
        """Set all recids that are assumed to exist by tasks of this master.

        :type recids: :py:class:`intbitset`
        """
        if self.all_recids is not None:
            raise Exception('Thou shall not set `all_recids` twice.')
        else:
            identifier = self.fmt(master_all_recids)
            self.conn.set(identifier, intbitset(recids).fastdump())

    @property
    def status(self):
        """Get the status of the master.

        :returns StatusMaster
        """
        identifier = self.fmt(master_status)
        return StatusMaster[self.conn.get(identifier)]

    @status.setter
    def status(self, new_status):
        """Set the status of the master.

        :type new_status: :py:class:`invenio_checker.redis_helpers.StatusMaster`
        """
        assert new_status in StatusMaster
        identifier = self.fmt(master_status)
        self.conn.set(identifier, new_status.name)


class StatusWorker(Enum):
    scheduled = 1
    booting = 2
    ready = 3
    running = 4
    terminated = 5


class RedisWorker(RedisClient):
    def __init__(self, task_id, eliot_task_id=None, bundle_requested_recids=None):
        """Instantiate a handle to all the manifestations of a worker.

        :type task_id: str
        """
        # TODO: Split to .create()
        assert (eliot_task_id is not None and bundle_requested_recids is not None) or (not eliot_task_id and not bundle_requested_recids)
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
        for master in self.conn.scan_iter('invenio_checker:master:*:workers'):
            if self.conn.sismember(master, self.task_id):
                master_id = master.split(':')[2]
                return RedisMaster(master_id)
        else:
            raise Exception("Can't find master!")

    def zap(self):
        """Zap this worker.

        This method is meant to be called from the master.
        """
        warn('Zapping worker' + str(self.task_id))
        if not self.result.ready():
            self.result.revoke(terminate=True, signal='TERM')
        self._cleanup()

    def _cleanup(self):
        """Remove this worker from redis."""
        for key in keys_worker:
            self.conn.delete(self.fmt(key))

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

    def _disappear_as_depender(self):
        """Remove this task from being a dependency of all other tasks."""
        self.lock.get()
        for worker in get_workers_in_redis():
            worker.retry_after_ids_pop(self.task_id)
        self.lock.release()

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

    @property
    def status(self):
        """Get the status of this worker.

        :rtype: :py:class:`StatusWorker`"
        """
        identifier = self.fmt(worker_status)
        return StatusWorker(int(self.conn.get(identifier)))

    @status.setter
    def status(self, new_status):
        """Set the status of this worker.

        :type new_status: :py:class:`invenio_checker.redis_helpers.StatusWorker`
        """
        # print 'SETTING STATUS {} ON {}'.format(new_status, self.task_id)

        assert new_status in StatusWorker

        identifier = self.fmt(worker_status)
        self.conn.set(identifier, new_status.value)

        if new_status == StatusWorker.terminated:
            self._disappear_as_depender()

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
        return self.bundle_requested_recids

    @_bundle_requested_recids.setter
    def _bundle_requested_recids(self, new_recids):
        """Set the recids that this worker shall iterate over.

        :type new_recids: :py:class:`intbitset`
        """
        identifier = self.fmt(worker_requested_recids)
        self.conn.set(identifier, intbitset(new_recids).fastdump())

    def clear_own_patches(self):
        identifier = client_patches.format(uuid=self.task_id, recid='*')
        for redis_patch in self.conn.scan_iter(identifier):
            self.conn.delete(redis_patch)


def make_fullpatch(recid, record_hash, patch, task_id):
    return {
        'recid': recid,
        'record_hash': record_hash,
        'patch': patch,
        'task_id': task_id
    }


def patch_to_redis(fullpatch):
    """
    Called on session finish, once per recid."""
    conn = _get_redis_conn()
    identifier = client_patches.format(
        uuid=fullpatch['task_id'],
        recid=fullpatch['recid'],
        record_hash=fullpatch['record_hash'],
    )
    assert not conn.get(identifier)
    conn.sadd(identifier, fullpatch['patch'].to_string())
    conn.expire(identifier, 365*86400)


def get_record_orig_or_mem(recid):
    conn = _get_redis_conn()
    record = get_record_orig(recid)
    identifier = client_patches.format(uuid='*', recid=recid, record_hash='*')
    sorted_patchsets = sorted(
        [conn.smembers(id_) for id_ in conn.scan_iter(identifier)],
        key=conn.ttl
    )

    for patchset in sorted_patchsets:
        for patch_str in patchset:
            # TODO: Check record hash
            jsonpatch.apply_patch(record, patch_str, in_place=True)
    return record
