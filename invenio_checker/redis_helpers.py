import six
import redis
from intbitset import intbitset  # pylint: disable=no-name-in-module
import time
from frozendict import frozendict
from redlock import Redlock
from uuid import uuid4
from enum import Enum

# PubSub
channel_where_worker_listens = 'invenio_checker:master:{task_id}'
channel_where_master_listens = 'invenio_checker:worker:{task_id}'

# Master
master_workers = 'invenio_checker:master:{master_id}:workers'
master_all_ids = 'invenio_checker:master:{master_id}:all_ids'
master_examining_lock = 'invenio_checker:master:examine_lock'
master_status = 'invenio_checker:master:{master_id}:status'

# Worker
worker_allowed_recids = 'invenio_checker:worker:{task_id}:allowed_recids'
worker_allowed_paths = 'invenio_checker:worker:{task_id}:allowed_paths'
worker_status = 'invenio_checker:worker:{task_id}:status'


# config['CACHE_REDIS_URL']  # FIXME
redis_uri_string = 'redis://localhost:6379/1'

def _get_redis_conn():
    return redis.StrictRedis.from_url(redis_uri_string)

def _get_all_masters(conn):
    return set([RedisMaster(master_worker.split(':')[2], instantiate=False)
                for master_worker in conn.scan_iter(master_workers.format(master_id='*'))])

def _get_lock_manager():
    return Redlock([redis_uri_string])

class Lock(object):

    @staticmethod
    def get():
        lock_manager = _get_lock_manager()
        while True:
            my_lock = lock_manager.lock(master_examining_lock, 1000*1000)  # FIXME: What do we do when it expires?
            if my_lock is False:
                print 'failed to get lock'; time.sleep(0.5)
            else:
                return my_lock

    @staticmethod
    def expired(my_lock, conn=None):
        if conn is None:
            conn = _get_redis_conn()
        # Note: my_lock.resource is `master_examining_lock`
        return conn.get(my_lock.resource) != my_lock.key

    @staticmethod
    def locked(conn=None):
        if conn is None:
            conn = _get_redis_conn()
        return bool(conn.get(master_examining_lock))

    @staticmethod
    def release(my_lock):
        # FIXME: Why dis happen
        if Lock.expired(my_lock):
            raise Exception('Oh no! Lock has already expired!')
        lock_manager = _get_lock_manager()
        lock_manager.unlock(my_lock)

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
        print '>', master.master_id, master.status.value, '<', StatusMaster.ready.value
        if master.status.value < StatusMaster.ready.value:
            print '..not even ready, skipping'
            # Master is not ready and is definitely not running this
            # function either.
            continue
        for worker in [RedisWorker(worker) for worker in master.workers]:
            print '<', worker.task_id, worker
            if worker.status == StatusWorker.running:
                running_workers[worker.task_id] = worker.conflict_dict
    masters_new = _get_all_masters(conn)
    assert masters_new == masters
    return running_workers


class RedisClient(object):
    def __init__(self):
        self.conn = _get_redis_conn()
        self.pubsub = self.conn.pubsub(ignore_subscribe_messages=True)


class StatusMaster(Enum):
    booting = 1
    ready = 2


class RedisMaster(RedisClient):
    # TODO: Init with master_id
    # And place in redis with master-clients relationship?
    def __init__(self, master_id=None, instantiate=True):
        from invenio_records.models import Record
        super(RedisMaster, self).__init__()
        if master_id is not None:
            self.master_id = master_id
        else:
            self.master_id = str(uuid4())
        if instantiate:
            self.all_recids = Record.allids()  # __init__ is a good time for this.
            self.status = StatusMaster.booting

    def zap(self):
        self.conn.delete(self.fmt(master_workers))
        self.conn.delete(self.fmt(master_all_ids))

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

    def sub_to_workers(self, task_ids, handler):
        self.pubsub.psubscribe(**{channel_where_master_listens.format(task_id=task_id):
                                  handler for task_id in task_ids})

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

    def __eq__(self, other):
        return self.master_id == other.master_id

    def __ne__(self, other):
        return self.master_id != other.master_id

    def __hash__(self):
        return hash(self.master_id)

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


class RedisWorker(RedisClient):
    def __init__(self, task_id):
        super(RedisWorker, self).__init__()
        self.task_id = task_id
        # self.master_id = master_id
        # Figure out master
        # for master in self.conn.scan_iter('invenio_checker:master:*:workers'):
        #     print '{}'.format(self.conn.smembers(master))
        #     if self.conn.sismember(master, task_id):
        #         self.master_id = master.split(':')[2]
        #         break
        #     else:
        #         raise Exception("Can't find master!")

    def fmt(self, string):
        # if self.master_id:
        #     return string.format(task_id=self.task_id, master_id=self.master_id)
        # else:
        return string.format(task_id=self.task_id)

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
        return self.pubsub.psubscribe(identifier)

    def pub_to_master(self, message):
        identifier = self.fmt(channel_where_master_listens)
        assert message in ('ready', )
        return self.conn.publish(identifier, message)

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

    # TODO: This is duplicate code from RedisMaster
    @staticmethod
    def get_master_all_recids(master_id):
        identifier = master_all_ids.format(master_id=master_id)
        conn = _get_redis_conn()
        return intbitset(conn.get(identifier))

    @property
    def ready(self):
        return self.allowed_recids is not None and self.allowed_paths is not None

    @property
    def status(self):
        identifier = self.fmt(worker_status)
        got = self.conn.get(identifier)
        if got is None:
            if self.ready:
                self.status = StatusWorker.ready
            else:
                self.status = StatusWorker.booting
            return self.status
        return StatusWorker(int(got))

    @status.setter
    def status(self, new_status):
        assert new_status in StatusWorker
        identifier = self.fmt(worker_status)
        self.conn.set(identifier, new_status.value)

    def __eq__(self, other):
        return self.task_id == other.task_id

    def __ne__(self, other):
        return self.task_id != other.task_id

    def __hash__(self):
        return hash(self.task_id)

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
