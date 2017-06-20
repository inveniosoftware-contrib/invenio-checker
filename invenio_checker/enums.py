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

from enum import Enum


class StatusWorker(Enum):  # pylint: disable=too-few-public-methods
    """Statuses a RedisWorker can be in."""
    dead_parrot = 0  # canary value
    scheduled = 1
    booting = 2
    ready = 3
    running = 4
    ran = 5
    failed = 6
    committed = 7


class StatusMaster(Enum):  # pylint: disable=too-few-public-methods
    """Statuses a RedisMaster can be in."""
    unknown = 0
    booting = 1
    running = 2
    waiting_to_commit = 3
    failed = 4
    completed = 5
