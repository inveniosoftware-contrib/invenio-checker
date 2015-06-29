# -*- coding: utf-8 -*-
##
## This file is part of SCOAP3.
## Copyright (C) 2015 CERN.
##
## SCOAP3 is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## SCOAP3 is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with SCOAP3; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Correct missing arxiv category."""

import re

from xml.dom.minidom import parseString
from xml.parsers.expat import ExpatError

from invenio.legacy.bibauthorid.general_utils import is_arxiv_id
from invenio.modules.jsonalchemy.reader import split_blob
from invenio.modules.records.api import Record


known_collections = {
    'ERRATUM': {'erratum'},
    'ADDENDUM': {'addendum'},
    'EDITORIAL': {'editorial'},
    'CORRIGENDUM': {'corrigendum'},
}


def _collections_in_title(title):
    title_words = set(re.findall(r'\w+', title))
    found_collections = set()  # until challenged
    for tag, transformations in known_collections.items():  # ERRATUM, erratum
        if title_words & transformations:
            found_collections ^= {tag}
    return found_collections


def _set_collections_in_record(record, found_collections):
    if not found_collections:
        return record
    elif len(found_collections) > 1:
        raise KeyError("Found multiple collection keywords in the title!")
    else:
        if 'collections' not in record:
            record['collections'] = {}
        record['collections']['additional'] = tuple(found_collections)[0]
        return record


def check_record(record):
    found_collections = _collections_in_title(record['title'])
    record = _set_collections_in_record(record, found_collections)
