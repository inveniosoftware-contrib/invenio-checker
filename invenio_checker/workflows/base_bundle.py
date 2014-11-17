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
import json
from invenio.modules.checker.rules import Rules


def spawn_queue(obj, eng):
    rule_jsons = obj.data['rule_jsons']
    ids = obj.data['ids']
    common = obj.data['common']
    rules = Rules.from_jsons(rule_jsons)
    for rule in rules:
        print rule.checkspec


class base_bundle(object):
    workflow = [spawn_queue]


__all__ = ['base_bundle']
