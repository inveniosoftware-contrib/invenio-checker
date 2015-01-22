# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2013, 2014 CERN.
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

from ..tasks.tasks import (
    ruledicts,
    run_batch,
    run_check,
    wf_recids,
    save_records
)
from invenio.modules.workflows.tasks.logic_tasks import foreach, end_for


class base_bundle(object):
    """Run a single rule bundle (IDs, rules) through the checks."""

    workflow = [
        foreach(ruledicts("batch"), "rule_object"),
        [
            run_batch,
            foreach(wf_recids(), "record"),
            [
                run_check
            ],
            end_for,
        ],
        end_for,
        foreach(ruledicts("simple"), "rule_object"),
        [
            foreach(wf_recids(), "record"),
            [
                run_check
            ],
            end_for,
        ],
        end_for,
        save_records()
    ]


__all__ = ('base_bundle', )
