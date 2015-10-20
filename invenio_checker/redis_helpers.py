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
import redis
from celery.result import AsyncResult
from celery.task.control import inspect  # pylint: disable=no-name-in-module, import-error
from six import string_types
from functools import wraps, partial
from redlock import RedLock

_prefix = 'invenio_checker'
prefix_master = _prefix + ':master:{uuid}'
prefix_worker = _prefix + ':worker:{uuid}'

# Generic
lock_last_key = prefix_worker + ':examine_lock'

# Common
client_eliot_task_id = _prefix + ':{uuid}:eliot_task_id'
client_patches = _prefix + ':patches:{uuid}:{recid}:{record_hash}' # patch


# config['CACHE_REDIS_URL']  # FIXME
redis_uri_string = 'redis://localhost:6379/1'

def get_lock_partial(identifier, conn):
    return partial(
        RedLock,
        identifier,
        connection_details=[conn.connection_pool.connection_kwargs]
    )

def get_all_tasks_in_celery(tries=3):
    """Get all celery tasks, even if they overlap.

    Overlapping may happen because celery has no atomic way of returning both
    active and scheduled tasks."""

    assert tries >= 0
    active = None
    scheduled = None
    while tries:
        tries -= 1
        insp = inspect()
        if active is None:
            active = insp.active()
        if scheduled is None:
            scheduled = insp.scheduled()
        if active is None or scheduled is None:
            continue
        return set(sum(active.values() + scheduled.values(), []))
    return {}

def get_redis_conn():
    """Return a connection to redis."""
    return redis.StrictRedis.from_url(redis_uri_string)

def _get_all_masters(conn):
    """Return all masters found in redis.

    :type conn: :py:class:`redis.client.StrictRedis`
    """
    from .master import RedisMaster, master_workers
    return [RedisMaster(master_worker.split(':')[2])
            for master_worker
            in conn.scan_iter(master_workers.format(uuid='*'))]

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
    """Get all master instances that have a key in redis."""
    from .master import RedisMaster, keys_master
    return [RedisMaster(master_id) for master_id in _get_things_in_redis(keys_master)]


def get_workers_in_redis():
    """Get all master instances that have a key in redis."""
    from .worker import RedisWorker, keys_worker
    return [RedisWorker(worker_id) for worker_id in _get_things_in_redis(keys_worker)]


def cleanup_failed_runs():
    all_tasks_in_celery = get_all_tasks_in_celery()
    for worker in get_workers_in_redis():
        if not worker.in_celery(all_tasks_in_celery):
            try:
                master = worker.master
            except AttributeError:
                worker.zap()
            else:
                master.zap()

def set_identifier(unformatted_identifier):
    """Set `self._identifier` of the passed function to the one it owns."""
    def tags_decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            self = args[0]
            self._identifier = self.fmt(unformatted_identifier)
            return func(*args, **kwargs)
        return func_wrapper
    return tags_decorator


class PleasePylint(object):
    """Make pylint think these are already set for use with mixins."""

    def __init__(self):
        if False:  # pragma: no cover
            self._identifier = None
            self.conn = None
            self.status = None
            self.uuid = None
            self.fmt = callable
            # worker only:
            self.lock = callable
            self.master = None
            self.task_id = None


class SetterProperty(object):
    """Setter for a property with not getter."""

    def __init__(self, func, doc=None):
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__

    def __set__(self, obj, value):
        return self.func(obj, value)


class RedisClient(PleasePylint):
    def __init__(self, uuid):
        """Initialize a lock handle for a certain client."""
        self.conn = get_redis_conn()
        self.uuid = uuid

    def create(self, eliot_task_id):
        self.eliot_task_id = eliot_task_id

    @property
    @set_identifier(client_eliot_task_id)
    def eliot_task_id(self):
        return self.conn.get(self._identifier)

    @eliot_task_id.setter
    @set_identifier(client_eliot_task_id)
    def eliot_task_id(self, new_id):
        return self.conn.set(self._identifier, new_id)

    def fmt(self, string):  # pylint: disable=method-hidden
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

    def in_celery(self, tasks_in_celery):
        """Return whether this client exists as a process in celery."""
        return any(task['id'] == self.uuid for task in tasks_in_celery)

    def __eq__(self, other):
        """Compare with other clients in a rich manner.

        :type other: str or RedisClient
        """
        if isinstance(other, string_types):
            return self.uuid == other
        return self.uuid == other.uuid

    def __ne__(self, other):
        """Compare with other clients in a rich manner.

        :type other: str or RedisClient
        """
        return not self == other

    def __hash__(self):
        """Return the hash of this client's UUID."""
        return hash(self.uuid)

    def __repr__(self):
        return "{}: {}".format(self.__class__, self.uuid)

