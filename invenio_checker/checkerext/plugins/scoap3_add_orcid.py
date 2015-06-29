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

"""Correct missing orcid."""

from collections import namedtuple
from invenio.legacy.bibauthorid.general_utils import is_valid_orcid
from harvestingkit.minidom_utils import xml_to_text
import re
from xml.parsers.expat import ExpatError
import sys
import os
from common import get_doc_ids, get_latest_file


def _get_orcids(xml_doc):
    orcid_pattern = '\d{4}-\d{4}-\d{4}-\d{3}[\d|X]'
    result = []

    def _append_orcid(orcid):
        if orcid and is_valid_orcid(orcid):
            result.append('ORCID:{0}'.format(orcid))
        else:
            result.append('')

    xml_authors = xml_doc.getElementsByTagName("ce:author")
    for xml_author in xml_authors:
        try:
            orcid = xml_author.getAttribute('orcid')
            _append_orcid(orcid)
        except IndexError:
            result.append('')
    if result:
        return result

    xml_authors = xml_doc.getElementsByTagName("contrib")
    for xml_author in xml_authors:
        try:
            contrib_id = xml_author.getElementsByTagName('contrib-id')[0]
            if contrib_id.getAttribute('contrib-id-type') == 'orcid':
                orcid_raw = xml_to_text(contrib_id)
                orcid = re.search(orcid_pattern, orcid_raw).group()
                _append_orcid(orcid)
        except (IndexError, AttributeError):
            result.append('')
    return result


def _set_orcid(author_dict, orcid):
    author_dict['orcid'] = orcid
    return author_dict


def check_record(record):
    doc_ids = get_doc_ids(record['recid'])
    for doc_id in doc_ids:
        latest_file = get_latest_file(doc_id)
        try:
            xml_doc = parse(latest_file)
        except (IOError, ExpatError):
            # TODO Log warning
            pass
        orcids = _get_orcids(xml_doc)
        for orcid, author_dict in zip(orcids, record['authors']):
            _set_orcid(author_dict, orcid)
