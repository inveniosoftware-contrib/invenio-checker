import six
import redis
from intbitset import intbitset  # pylint: disable=no-name-in-module
import time


# PubSub
channel_where_worker_listens = 'invenio_checker:master:{task_id}'
channel_where_master_listens = 'invenio_checker:worker:{task_id}'

# Master
master_workers = 'invenio_checker:master:{master_id}:workers'
master_all_ids = 'invenio_checker:master:{master_id}:all_ids'

# Worker
worker_allowed_recids = 'invenio_checker:worker:{task_id}:allowed_recids'
worker_allowed_paths = 'invenio_checker:worker:{task_id}:allowed_paths'


def _get_redis_conn():
    def redis_args_from_uri(url):
        from urlparse import urlparse
        redis_uri = urlparse(url)
        return {
            'host': redis_uri.hostname,
            'port': redis_uri.port,
            'db': int(redis_uri.path.split('/')[1])
        }

    # config['CACHE_REDIS_URL']  # FIXME
    redis_uri_string = 'redis://localhost:6379/1'

    redis_args = redis_args_from_uri(redis_uri_string)
    return redis.StrictRedis(**redis_args)


class RedisClient(object):
    def __init__(self):
        self.conn = _get_redis_conn()
        self.pubsub = self.conn.pubsub(ignore_subscribe_messages=True)


class RedisMaster(RedisClient):
    # TODO: Init with master_id
    # And place in redis with master-clients relationship?
    def __init__(self, master_id):
        super(RedisMaster, self).__init__()
        self.master_id = master_id
        from invenio.modules.records.models import Record
        self.all_recids = Record.allids()  # __init__ is a good time for this.

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
        return intbitset(self.conn.get(identifier))

    @all_recids.setter
    def all_recids(self, recids):
        try:
            self.all_recids
        except TypeError:  # from intbitset cast. XXX Clunky
            identifier = self.fmt(master_all_ids)
            self.conn.set(identifier, intbitset(recids).fastdump())
        else:
            raise Exception('Thou shall not set `all_recids` twice.')


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
        return self.conn.smembers(identifier)

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
