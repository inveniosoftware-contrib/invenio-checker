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

import time
import jsonpatch
import redis
import redlock
from celery.result import AsyncResult
from celery.task.control import inspect  # pylint: disable=no-name-in-module, import-error
from enum import Enum
from intbitset import intbitset  # pylint: disable=no-name-in-module
from six import string_types
from warnings import warn
from invenio_records.api import get_record as get_record_orig
from collections import defaultdict
from operator import itemgetter

_prefix = 'invenio_checker'
prefix_master = _prefix + ':master:{uuid}'
prefix_worker = _prefix + ':worker:{uuid}'

# Generic
master_examining_lock = _prefix + ':master:examine_lock'
lock_last_owner = prefix_worker + ':examine_lock'

# Common
client_eliot_task_id = _prefix + ':{uuid}:eliot_task_id'
client_patches = _prefix + ':patches:{uuid}:{recid}:{record_hash}' # patch


# config['CACHE_REDIS_URL']  # FIXME
redis_uri_string = 'redis://localhost:6379/1'


# class RedisConn(object):
#     redis_conn = redis.StrictRedis.from_url(redis_uri_string)


def get_all_values_in_celery():
    insp = inspect()
    active = None
    scheduled = None
    while active is None:
        active = insp.active()
    while scheduled is None:
        scheduled = insp.scheduled()
    return active.values() + scheduled.values()


def get_redis_conn():
    # return RedisConn.redis_conn
    return redis.StrictRedis.from_url(redis_uri_string)


def _get_all_masters(conn):
    """Return all masters found in redis.

    :type conn: :py:class:`redis.client.StrictRedis`
    """
    from .master import RedisMaster, master_workers
    return set([RedisMaster(master_worker.split(':')[2])
                for master_worker in conn.scan_iter(master_workers.format(uuid='*'))])


def _get_lock_manager():
    """Get an instance of redlock for the configured redis servers."""
    return redlock.Redlock([redis_uri_string])


def _get_things_in_redis(prefixes):
    """Return the IDs of all masters or workers in redis.

    :type prefixes: set

    :rtype: set
    """
    kwargs = {'uuid': '*'}
    conn = get_redis_conn()
    ids = set()
    for pref in prefixes:
        for field in conn.scan_iter(pref.format(**kwargs)):
            id_ = field.split(':')[2]
            ids.add(id_)
    return ids


def get_masters_in_redis():
    from .master import RedisMaster, keys_master
    return set([RedisMaster(master_id) for master_id in _get_things_in_redis(keys_master)])


def get_workers_in_redis():
    from .worker import RedisWorker, keys_worker
    return set([RedisWorker(worker_id) for worker_id in _get_things_in_redis(keys_worker)])


def cleanup_failed_runs():
    all_values_in_celery = get_all_values_in_celery()
    for worker in get_workers_in_redis():
        if not worker.in_celery(all_values_in_celery):
            try:
                master = worker.master
            except AttributeError:
                worker.zap()
            else:
                master.zap()


class Lock(object):

    def __init__(self, master_id):
        """Initialize a lock handle for a certain master.

        :type master_id: str
        """
        self.conn = get_redis_conn()
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
        self.conn = get_redis_conn()
        self.uuid = uuid
        if eliot_task_id is not None:
            self.eliot_task_id = eliot_task_id

    @property
    def eliot_task_id(self):
        identifier = self.fmt(client_eliot_task_id)
        return self.conn.get(identifier)

    @eliot_task_id.setter
    def eliot_task_id(self, new_id):
        identifier = self.fmt(client_eliot_task_id)
        return self.conn.set(identifier, new_id)

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

    def in_celery(self, values_in_celery):
        """Return whether this client exists as a process in celery."""
        for alive_tasks in values_in_celery:
            for alive_task in alive_tasks:
                if alive_task['id'] == self.uuid:
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

    def __repr__(self):
        return "{}: {}".format(self.__class__, self.uuid)

