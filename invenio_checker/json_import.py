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

"""
The JSON <--> DB objects (sqlalchemy) converter module.
"""

import json


def json2models(json_file, model_name):
    """
    Given a JSON file and a model name, generates the corresponding objects.
    """
    model = __import__('models', globals(), locals(), [model_name])
    with open(json_file) as f:
        for entry in json.load(f):
            yield model(**entry)


def models2json(db_objs, json_file):
    """
    Given an iterable of model objects, dumps them to the JSON file specified.
    """
    serialized_list = []
    for db_obj in db_objs:
        db_obj_dict = {}
        for col in db_obj.__table__.columns:
            try:
                db_obj_dict[col.name] = col.__getattr__(col.name)
            except AttributeError as e:
                pass
        serialized_list.append(db_obj_dict)
    with open(json_file, 'w') as f:
        json.dump(serialized_list, f)
