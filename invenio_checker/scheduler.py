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
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

from invenio.ext.sqlalchemy import db
from invenio.base.factory import with_app_context


@with_app_context()
def get_scheduler():

    jobstores = {
        'default': SQLAlchemyJobStore(engine=db.engine,
                                      tablename='checker_apscheduler_jobs')
    }
    executors = {
        'default': ThreadPoolExecutor()
    }
    job_defaults = {
        'coalesce': True,
        'max_instances': 1,
    }

    scheduler = BackgroundScheduler(jobstores=jobstores,
                                    executors=executors,
                                    job_defaults=job_defaults)

    return scheduler

# scheduler.remove_all_jobs()
# scheduler.add_job(str('invenio_checker.scheduler:write_thing'), 'interval', seconds=5)
