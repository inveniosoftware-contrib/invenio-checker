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

import signal
from intbitset import intbitset  # pylint: disable=no-name-in-module
from .redis_helpers import (
    RedisClient,
    get_redis_conn,
    prefix_master,
)
from warnings import warn
from .enums import StatusMaster, StatusWorker

from invenio.ext.sqlalchemy import db

# Master
master_all_recids = prefix_master + ':all_recids'
master_status = prefix_master + ':status'
master_rule_name = prefix_master + ':rule_name'
master_workers = prefix_master + ':workers'


# References
keys_master = {
    master_workers,
    master_all_recids,
    master_status,
    master_rule_name,
}


class HasWorkers(object):
    # TODO: Return actual workers (redis_workers)
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
        from .worker import RedisWorker
        return {RedisWorker(worker_id) for worker_id in self.workers}

    def workers_append(self, worker_id):
        identifier = self.fmt(master_workers)
        self.conn.sadd(identifier, worker_id)

    def worker_status_changed(self):
        if any(w.status == StatusWorker.failed for w in self.redis_workers):
            self.status = StatusMaster.failed
        elif all(w.status == StatusWorker.committed for w in self.redis_workers):
            self.status = StatusMaster.completed


class HasAllRecids(object):

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


class HasStatusMaster(object):

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

        execution = self.get_execution()
        execution.status = self.status
        db.session.add(execution)
        db.session.commit()

    def get_execution(self):
        from .models import CheckerRuleExecution
        return CheckerRuleExecution.query.get(self.uuid)


class HasRuleName(object):

    @property
    def rule_name(self):
        identifier = self.fmt(master_rule_name)
        return self.conn.get(identifier)

    @property
    def _rule_name(self):
        return self.rule_name

    @_rule_name.setter
    def _rule_name(self, new_rule_name):
        identifier = self.fmt(master_rule_name)
        self.conn.set(identifier, new_rule_name)

    @property
    def rule(self):
        from .models import CheckerRule
        return CheckerRule.query.get(self.rule_name)


class RedisMaster(RedisClient, HasRuleName, HasWorkers, HasAllRecids, HasStatusMaster):
    def __init__(self, master_id, eliot_task_id=None, rule_name=None):
        """Initialize a lock handle for a certain master.

        ..note::
            Registers the master with redis if it does not already exist.

        :type master_id: str
        """
        from invenio_records.models import Record
        super(RedisMaster, self).__init__(master_id, eliot_task_id)

        if not RedisMaster._already_instnatiated(master_id):
            self.all_recids = Record.allids()  # __init__ is a good time for this.
            self.status = StatusMaster.booting
            self._rule_name = rule_name

    @property
    def master_id(self):
        return self.uuid

    @staticmethod
    def _already_instnatiated(master_id):
        """Return whether this master is already registered with redis.

        :type master_id: str
        """
        conn = get_redis_conn()
        for prefix in keys_master:
            for field in conn.scan_iter(prefix.format(uuid='*')):
                id_ = field.split(':')[2]
                if id_ == master_id:
                    return True
        return False

    def _cleanup(self):
        """Remove this worker from redis."""
        for key in keys_master:
            self.conn.delete(self.fmt(key))

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
