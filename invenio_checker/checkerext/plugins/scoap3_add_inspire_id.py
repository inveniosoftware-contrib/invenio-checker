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


def get_inspire_id(record):
    try:
        system_control_numbers = record['system_control_number']
    except KeyError:
        return

    instances_found = []
    for system_control_number in system_control_numbers:
        try:
            if system_control_number['institute'] == 'INSPIRE':
                instances_found.append(system_control_number)
        except KeyError:
            pass

    if len(instances_found) > 1:
        raise KeyError('Multiple institutes identify as INSPIRE!')
    try:
        return instances_found[0]
    except IndexError:
        return None


def get_inspireid_from_database(doi):
    from invenio.ext.sqlalchemy import db  # Keep this here for the sake of mock
    return db.session.query('FIXME').filter('FIXME').one()


def get_inspireid_from_inspire_net(doi=None, arxiv=None):
    import urllib
    import urllib2
    if not any((doi, arxiv)):
        raise TypeError('You must provide `doi` or `arxiv` or both!')

    query_args = {}
    for key, val in {'doi': doi, 'arxiv': arxiv}.items():
        if val is not None:
            query_args[key] = val
    url = 'http://inspirehep.net/search'
    url_values = urllib.urlencode(query_args)
    full_url = url + '?' + url_values
    try:
        data = urllib2.urlopen(full_url).read()
    except (IOError, TypeError):
        # TODO: Log warning
        raise
    match = re.match('\[((?P<id>\d+)(, )?)*\]', data)
    if not match:
        # TODO: Log warning
        raise TypeError
    empty_match = not match.group('id')
    if empty_match:
        return None

    returned_ids = tuple(int(id_.strip()) for id_ in data[1:-1].split(','))
    if len(returned_ids) > 1:
        # TODO: Log warning
        return None
    try:
        return returned_ids[0]
    except IndexError:
        return None

# TODO: check_record()
