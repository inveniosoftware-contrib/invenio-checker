import time

import redis
import redlock
from celery import states
from celery.result import AsyncResult
from celery.task.control import inspect  # pylint: disable=no-name-in-module
from enum import Enum
from intbitset import intbitset  # pylint: disable=no-name-in-module
from invenio_records.models import Record
from six import string_types
from warnings import warn


prefix = 'invenio_checker'
prefix_worker = prefix + ':worker:{task_id}'
prefix_master = prefix + ':master:{master_id}'

# Global
master_examining_lock = prefix + ':master:examine_lock'

# Pubsub
channel_where_master_listens = 'invenio_checker:worker:{task_id}'
channel_where_worker_listens = 'invenio_checker:master:{task_id}'

# Master
master_workers = prefix_master + ':workers'
master_all_ids = prefix_master + ':all_ids'
master_status = prefix_master + ':status'
master_last_lock = prefix_master + ':examine_lock'

# Worker
worker_allowed_recids = prefix_worker + ':allowed_recids'
worker_allowed_paths = prefix_worker + ':allowed_paths'
worker_requested_recids = prefix_worker + ':requested_recids'
worker_status = prefix_worker + ':status'

keys_master = {master_workers, master_all_ids, master_status}
keys_worker = {worker_allowed_recids, worker_allowed_paths, worker_requested_recids, worker_status}

# config['CACHE_REDIS_URL']  # FIXME
redis_uri_string = 'redis://localhost:6379/1'


# class RedisConn(object):
#     redis_conn = redis.StrictRedis.from_url(redis_uri_string)


# https://mail.python.org/pipermail/python-ideas/2011-January/008958.html
class staticproperty(object):
    """Property decorator for static methods."""

    def __init__(self, function):
        self._function = function

    def __get__(self, instance, owner):
        return self._function()


def _get_redis_conn():
    # return RedisConn.redis_conn
    return redis.StrictRedis.from_url(redis_uri_string)


def _get_all_masters(conn):
    return set([RedisMaster(master_worker.split(':')[2], instantiate=False)
                for master_worker in conn.scan_iter(master_workers.format(master_id='*'))])


def _get_lock_manager():
    return redlock.Redlock([redis_uri_string])


def get_running_workers():
    """Return all running_workers which are at in, at least, ready state in the format
    they report allowed_paths and allowed_recids.

    ..note::
        This function will infinitely wait for non-ready running_workers to become ready.
    """
    conn = _get_redis_conn()
    masters = _get_all_masters(conn)
    running_workers = {}
    for master in masters:
        # print '>', master.master_id, master.status.value, '<', StatusMaster.ready.value
        for worker in [RedisWorker(worker) for worker in master.workers]:
            # print '<', worker.task_id, worker
            print worker.status
            if worker.status == StatusWorker.running:
                running_workers[worker.task_id] = worker.conflict_dict
    masters_new = _get_all_masters(conn)
    assert masters_new == masters
    return running_workers


def get_things_in_redis(prefixes):
    conn = _get_redis_conn()
    ids = set()
    for prefix in prefixes:
        for field in conn.scan_iter(prefix.format(master_id='*', task_id='*')):
            id_ = field.split(':')[2]
            ids.add(id_)
    return ids


def get_masters_in_redis():
    return set([RedisMaster(i, instantiate=False) for i in get_things_in_redis(keys_master)])


def get_workers_in_redis():
    return set([RedisWorker(i) for i in get_things_in_redis(keys_worker)])


def cleanup_failed_runs():
    # from celery.contrib import rdb; rdb.set_trace()
    slept = False
    for master in get_masters_in_redis():
        if not master.in_celery:
            if not slept:
                time.sleep(2)
                slept = True
            master.zap(force=True, sleep_before_kill=0)
    for worker in get_workers_in_redis():
        if not worker.in_celery:
            if not slept:
                time.sleep(2)
                slept = True
            worker.zap(force=True, sleep_before_kill=0)


class Lock(object):

    def __init__(self, master_id):
        self.conn = _get_redis_conn()
        self.master_id = master_id
        self._lock_manager = _get_lock_manager()

    @property
    def _lock(self):
        ret = self.conn.hgetall(master_last_lock)
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
        self.conn.hmset(
            master_last_lock,
            {
                'last_master_id': self.master_id,
                'lock_validity': tuple_.validity,
                'lock_resource': tuple_.resource,
                'lock_key': tuple_.key
            }
        )

    def get(self):
        while True:
            new_lock = self._lock_manager.lock(master_examining_lock, 1000*1000)
            persisted = self.conn.persist(master_examining_lock)
            assert persisted
            if new_lock is False:
                print 'Failed to get lock, trying again.'
                time.sleep(0.5)
            else:
                self._lock = new_lock
                break

    def release(self, check_expired=True):
        if self._lock['last_master_id'] != self.master_id:
            return
        self._lock_manager.unlock(self._lock['tuple'])


class RedisClient(object):
    def __init__(self):
        self.conn = _get_redis_conn()
        self.pubsub = self.conn.pubsub(ignore_subscribe_messages=True)

    @property
    def uuid(self):
        raise NotImplementedError

    @property
    def result(self):
        return AsyncResult(id=self.uuid)

    @property
    def in_celery(self):
        # if self.result.state == states.STARTED:
        insp = inspect()
        active = insp.active()
        for active_tasks in active.values():
            for active_task in active_tasks:
                if active_task['id'] == self.uuid:
                    return True
        return False

    def __eq__(self, other):
        if isinstance(other, string_types):
            return self.uuid == other
        return self.uuid == other.uuid

    def __ne__(self, other):
        if isinstance(other, string_types):
            return self.uuid == other
        return self.uuid != other.uuid

    def __hash__(self):
        return hash(self.uuid)


class StatusMaster(Enum):
    booting = 1
    ready = 2


class RedisMaster(RedisClient):
    def __init__(self, master_id, instantiate=True):
        super(RedisMaster, self).__init__()

        self.master_id = master_id
        self.lock = Lock(self.master_id)

        if not RedisMaster._already_instnatiated(master_id):
            self.all_recids = Record.allids()  # __init__ is a good time for this.
            self.status = StatusMaster.booting

    @property
    def uuid(self):
        return self.master_id

    @staticmethod
    def _already_instnatiated(master_id):
        conn = _get_redis_conn()
        for prefix in keys_master:
            for field in conn.scan_iter(prefix.format(master_id='*', task_id='*')):
                id_ = field.split(':')[2]
                if id_ == master_id:
                    return True
        return False

    def zap(self, force=False, sleep_before_kill=2):
        # FIXME: Should we lock here?
        warn('Zapping ' + str(self.master_id))
        # Release lock
        self.lock.release(check_expired=False)
        if force:
            time.sleep(sleep_before_kill)
            self.result.revoke(terminate=True, signal='TERM')
            time.sleep(sleep_before_kill)
            self.result.revoke(terminate=True, signal='KILL')
        for key in keys_master:
            self.conn.delete(self.fmt(key))
        # Kill workers
        for redis_worker in self.redis_workers:
            redis_worker.zap(force=force, sleep_before_kill=False)


    @property
    def exited(self):
        for key in keys_master:
            if self.conn.get(self.fmt(key)):
                return True
        return False

    def fmt(self, string):
        return string.format(master_id=self.master_id)

    @property
    def workers(self):
        identifier = self.fmt(master_workers)
        return self.conn.smembers(identifier)

    @workers.setter
    def workers(self, worker_ids):
        identifier = self.fmt(master_workers)
        self.conn.delete(identifier)
        if worker_ids:
            self.conn.sadd(identifier, *worker_ids)

    @property
    def redis_workers(self):
        return {RedisWorker(worker_id) for worker_id in self.workers}

    # def sub_to_workers(self, task_ids):
    #     self.pubsub.subscribe(*(channel_where_master_listens.format(task_id=task_id)
    #                             for task_id in task_ids))

    def pub_to_worker(self, task_id, message):
        assert message in ('run', )
        return self.conn.publish(channel_where_worker_listens.format(task_id=task_id),
                                 message)

    @property
    def all_recids(self):
        identifier = self.fmt(master_all_ids)
        recids_set = self.conn.get(identifier)
        if recids_set is None:
            return None
        return intbitset(recids_set)

    @all_recids.setter
    def all_recids(self, recids):
        if self.all_recids is not None:
            raise Exception('Thou shall not set `all_recids` twice.')
        else:
            identifier = self.fmt(master_all_ids)
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

        :param new_status: StatusMaster
        """
        assert new_status in StatusMaster
        identifier = self.fmt(master_status)
        self.conn.set(identifier, new_status.name)


class StatusWorker(Enum):
    ready = 1
    booting = 2
    running = 3
    terminated = 4


class RedisWorker(RedisClient):
    def __init__(self, task_id):
        super(RedisWorker, self).__init__()
        self.task_id = task_id
        # Figure out master

    @property
    def uuid(self):
        return self.task_id

    @property
    def master(self):
        for master in self.conn.scan_iter('invenio_checker:master:*:workers'):
            if self.conn.sismember(master, self.task_id):
                master_id = master.split(':')[2]
                return RedisMaster(master_id, instantiate=False)
        else:
            raise Exception("Can't find master!")

    def fmt(self, string):
        return string.format(task_id=self.task_id)

    def zap(self, force=False, sleep_before_kill=2):
        warn('Zapping ' + str(self.task_id))
        if force:
            time.sleep(sleep_before_kill)
            self.result.revoke(terminate=True, signal='TERM')
            time.sleep(sleep_before_kill)
            self.result.revoke(terminate=True, signal='KILL')
        for key in keys_worker:
            self.conn.delete(self.fmt(key))

    @property
    def allowed_recids(self):
        identifier = self.fmt(worker_allowed_recids)
        recids = self.conn.get(identifier)
        if recids is None:
            return None
        return intbitset(self.conn.get(identifier))

    @allowed_recids.setter
    def allowed_recids(self, allowed_recids):
        identifier = self.fmt(worker_allowed_recids)
        self.conn.set(identifier, intbitset(allowed_recids).fastdump())

    def sub_to_master(self):
        identifier = self.fmt(channel_where_worker_listens)
        return self.pubsub.subscribe(identifier)

    # def pub_to_master(self, message):
    #     identifier = self.fmt(channel_where_master_listens)
    #     assert message in ('ready', )
    #     return self.conn.publish(identifier, message)

    @property
    def allowed_paths(self):
        identifier = self.fmt(worker_allowed_paths)
        paths = self.conn.smembers(identifier)
        if paths is None:
            return None
        return frozenset(paths)

    @allowed_paths.setter
    def allowed_paths(self, allowed_paths):
        identifier = self.fmt(worker_allowed_paths)
        self.conn.delete(identifier)
        if allowed_paths:
            self.conn.sadd(identifier, *allowed_paths)

    @property
    def status(self):
        # Check for terminated
        if self.result.state in states.READY_STATES:
            self.status = StatusWorker.terminated

        # Check for other statuses
        identifier = self.fmt(worker_status)
        got = self.conn.get(identifier)
        # TODO: No implicit?
        if got is None:
            self.status = StatusWorker.booting
            return self.status
        return StatusWorker(int(got))

    @status.setter
    def status(self, new_status):
        assert new_status in StatusWorker
        identifier = self.fmt(worker_status)
        self.conn.set(identifier, new_status.value)

    @property
    def bundle_requested_recids(self):
        identifier = self.fmt(worker_requested_recids)
        recids = self.conn.get(identifier)
        if recids is None:
            return None
        return intbitset(self.conn.get(identifier))


    @bundle_requested_recids.setter
    def bundle_requested_recids(self, new_recids):
        identifier = self.fmt(worker_requested_recids)
        self.conn.set(identifier, intbitset(new_recids).fastdump())

    @property
    def conflict_dict(self):
        """Dictionary for use with resolving conflicting workers."""
        return {
            'allowed_paths': self.allowed_paths,
            'allowed_recids': self.allowed_recids
        }


def capped_intervalometer(timeout, interval,
                          while_, do=None,
                          on_timeout=None):
    time_slept = 0
    while while_():
        if do:
            do()
        time.sleep(interval)
        time_slept += interval
        if time_slept >= timeout:
            if on_timeout:
                on_timeout()
            break
