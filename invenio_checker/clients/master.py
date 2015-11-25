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
    PleasePylint,
    set_identifier,
    SetterProperty,
)
from warnings import warn
from invenio_checker.enums import StatusMaster, StatusWorker

from invenio.ext.sqlalchemy import db  # pylint: disable=no-name-in-module

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


class HasWorkers(PleasePylint):
    """Give master the ability to set and reason about its workers."""

    @property
    @set_identifier(master_workers)
    def workers(self):
        """Get all the workers associated with this master.

        :rtype: set of :py:class:`WorkerRedis`
        """
        from .worker import RedisWorker
        return {RedisWorker(worker_id) for worker_id in self.conn.smembers(self._identifier)}

    @workers.setter
    @set_identifier(master_workers)
    def workers(self, worker_ids):
        """Associate workers with this master.

        :type worker_ids: set of IDs
        """
        self.conn.delete(self._identifier)
        if worker_ids:
            return self.conn.sadd(self._identifier, *worker_ids)

    @set_identifier(master_workers)
    def workers_append(self, worker_id):
        """Append a worker to this master given its identifier.

        :type worker_id: str
        """
        return self.conn.sadd(self._identifier, worker_id)

class HasAllRecids(PleasePylint):

    @property
    @set_identifier(master_all_recids)
    def all_recids(self):
        """Get all recids that are assumed to exist by tasks of this master."""
        recids_set = self.conn.get(self._identifier)
        if recids_set is None:
            return None
        return intbitset(recids_set)

    @all_recids.setter
    @set_identifier(master_all_recids)
    def all_recids(self, recids):
        """Set all recids that are assumed to exist by tasks of this master.

        :type recids: :py:class:`intbitset`
        """
        if self.all_recids is not None:
            raise Exception('Do not set `all_recids` twice. You will break '
                            'conflict resolution if a worker has already '
                            'started.')
        else:
            return self.conn.set(self._identifier, intbitset(recids).fastdump())


class HasStatusMaster(PleasePylint):

    @property
    @set_identifier(master_status)
    def status(self):
        """Get the status of the master.

        :returns StatusMaster
        """
        return StatusMaster[self.conn.get(self._identifier)]

    @status.setter
    @set_identifier(master_status)
    def status(self, new_status):
        """Set the status of the master.

        :type new_status: :py:class:`invenio_checker.redis_helpers.StatusMaster`
        """
        assert new_status in StatusMaster
        self.conn.set(self._identifier, new_status.name)

        execution = self.get_execution()
        execution.status = self.status
        try:
            db.session.add(execution)
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise

    def get_execution(self):
        from invenio_checker.models import CheckerRuleExecution
        return CheckerRuleExecution.query.filter(
                CheckerRuleExecution.uuid == self.uuid).one()


class HasRule(PleasePylint):
    """Gives master knowledge about the rule it's assigned."""

    @property
    @set_identifier(master_rule_name)
    def rule_name(self):
        return self.conn.get(self._identifier)

    @SetterProperty
    @set_identifier(master_rule_name)
    def _rule_name(self, new_rule_name):
        return self.conn.set(self._identifier, new_rule_name)

    @property
    def rule(self):
        from invenio_checker.models import CheckerRule
        return CheckerRule.query.get(self.rule_name)


class RedisMaster(RedisClient, HasRule, HasWorkers, HasAllRecids, HasStatusMaster):
    def __init__(self, master_id, eliot_task_id=None, rule_name=None):
        """Initialize a lock handle for a certain master.

        ..note::
            Registers the master with redis if it does not already exist.

        :type master_id: str
        """
        super(RedisMaster, self).__init__(master_id)

    @classmethod
    def create(cls, master_id, eliot_task_id, rule_name):
        from invenio_records.models import Record
        self = cls(master_id)
        self.status = StatusMaster.booting
        self.all_recids = Record.allids()  # create is a good time for this.
        self._rule_name = rule_name
        super(RedisMaster, self).create(eliot_task_id)
        return self

    @property
    def master_id(self):
        return self.uuid

    def _cleanup(self):
        """Remove this worker from redis."""
        for key in keys_master:
            self.conn.delete(self.fmt(key))

    def zap(self):
        """Zap this master and its workers.

        This method is meant to be called by a SIGINT or exception handler from
        within the master process itself.
        """
        signal.signal(signal.SIGINT, lambda rcv_signal, frame: None)
        # Kill workers
        for worker in self.workers:
            worker.zap()
        warn('Zapping master ' + str(self.master_id))
        self._cleanup()
