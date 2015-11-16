# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2015 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

class ReporterA(object):

    def __init__(self, task_name):
        self.task_name = task_name

    def report_exception(self, when, outrep_summary, location_tuple, formatted_exception=None, patches=None):
        pass

    def report(self, user_readable_msg, location_tuple=None):
        pass

    def finalize(self):
        pass

def get_reporter(task_name):
    return ReporterA(task_name)
