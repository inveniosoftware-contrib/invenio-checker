# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA

from __future__ import unicode_literals

import os
import errno
from celery.schedules import crontab

from flask import current_app

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def get_eliot_log_path():

    eliot_log_path = os.path.join(
        current_app.instance_path,
        current_app.config.get('CFG_LOGDIR', ''),
        'checker',
    )

    mkdir_p(eliot_log_path)

    return eliot_log_path

CHECKER_CELERYBEAT_SCHEDULE = {

    'checker-beat': {
        'task': 'invenio_checker.supervisor.beat',
        'schedule': crontab(minute='*/1'),
    }
}
