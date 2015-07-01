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

import urllib
from xml.dom.minidom import parseString
from xml.parsers.expat import ExpatError

from invenio.legacy.bibauthorid.general_utils import is_arxiv_id
from invenio.modules.jsonalchemy.reader import split_blob
from invenio.modules.records.api import Record


def _get_arxiv_categories_from_arxiv_org(id_):
    """
    :raises: IOError
    """
    url_values = urllib.urlencode({'search_query': 'id:' + id_})
    url = 'http://export.arxiv.org/api/query?' + url_values
    try:
        data = urllib.urlopen(url.format(id)).read()
        xml = parseString(data)
    except (IOError, ExpatError):
        raise IOError
    else:
        result = []
        for tag in xml.getElementsByTagName('category'):
            try:
                result.append(tag.attributes['term'].value)
            except KeyError:
                return None
        return set(result)


def _get_arxiv_id_from_record(record):
    """
    :raises: KeyError
    """
    try:
        primary_report_number = record['primary_report_number']
    except KeyError:
        return None
    else:
        if is_arxiv_id(primary_report_number):
            arxiv_id = re.sub('^arXiv:', '', primary_report_number)
            return arxiv_id
        else:
            return None


# def _recid_to_doi(recid):
#     try:
#         doi = get_record(recid)['doi']
#     except KeyError:
#         return None


def _get_arxiv_id_from_inspire(doi):
    """
    """
    url_values = urllib.urlencode({'p': 'doi', 'doi': doi, 'of': 'xm'})
    url = 'https://inspirehep.net/search?' + url_values
    try:
        collectionxml = urllib.urlopen(url).read()
    except IOError:
        raise
    else:
        try:
            recordxml = list(split_blob(collectionxml, 'marc'))[0]
        except IndexError:
            return None
        inspire_record = Record.create(recordxml, master_format='marc',
                                       namespace='recordext')
        return _get_arxiv_id_from_record(inspire_record)


def _add_arxiv_to_record(record, arxiv_id):
    # TODO Not that the ID was set by us (versys taken from inspire)
    record['primary_report_number'] = 'arXiv:' + arxiv_id


def _get_arxiv_id(record):
    arxiv_id = _get_arxiv_id_from_record(record)
    if arxiv_id:
        return arxiv_id
    else:
        try:
            arxiv_id = _get_arxiv_id_from_inspire(record)
        except IOError:
            # TODO: Log network failure
            return None
        else:
            _add_arxiv_to_record(record, arxiv_id)
            return arxiv_id


# def _get_category_position(record):
#     for position, val in record.iterfield('591__a'):
#         if 'Category:' in val:
#             return position


# def _has_category_field(record):
#     return any(filter(lambda x: 'Category'
#                in x[1], record.iterfield('591__a')))


def check_record(record):
    # Store 'Category:1' if found, else 'Category:0'
    string = 'Category:{0}'
    arxiv_id = _get_arxiv_id(record)
    if arxiv_id:
        try:
            categories = _get_arxiv_categories_from_arxiv_org(arxiv_id)
        except IOError:
            categories = set()
        if categories:
            val = string.format(int(any(filter(lambda x: 'hep' in x,
                                                categories))))
            # FIXME: Add to database
            # if _has_category_field(record):
            #     position = _get_category_position(record)
            #     record.amend_field(position, val, '')
            # else:
            #     record.add_field('591__a', value='', subfields=[('a', val)])
        else:
            pass
            # FIXME
            # record.warn("Problem checking arXiv category.")
    return record
