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
from invenio.modules.checker.rules import Rules
from invenio.modules.workflows.api import start_delayed
from invenio.modules.workflows.models import BibWorkflowObject
from invenio.modules.workflows.worker_result import uuid_to_workflow
from invenio.modules.workflows.engine import BibWorkflowEngine


def spawn_queue(obj, eng):
    """Spawns a queue for a single rule bundle - ids set.

    :param obj: ids, common data, Rules in json
    :type  obj: dict
    :param eng: workflow engine
    :type  eng: BibWorkflowEngine

    :returns: TODO
    :rtype:   TODO

    :raises: TODO
    """
    # Load rules from obj
    rules = Rules.from_jsons(obj.data['rule_jsons'])

    # Run batch
    batch_objects = []
    for rule in rules.iterbatch():
        batch_object = BibWorkflowObject.create_object()
        batch_object.set_data({
            'rule_json': rule.to_json(),
            'ids': obj.data['ids'],
            'common': obj.data['common']
        })
        batch_object.save()
        batch_objects.append(batch_object)
    asyncr = start_delayed(workflow_name="batch", data=batch_objects)
    wf = asyncr.get(uuid_to_workflow)
    engine = BibWorkflowEngine(uuid=wf.uuid)
    # for batch_object in engine.completed_objects:
    #     print batch_object.get_data()

class base_bundle(object):
    workflow = [spawn_queue]


__all__ = ['base_bundle']
