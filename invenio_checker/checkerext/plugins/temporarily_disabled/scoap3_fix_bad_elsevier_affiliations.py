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
from harvestingkit.minidom_utils import xml_to_text, get_value_in_tag
from common import get_doc_ids, get_latest_file

# TODO:
# searching in checker


#################################################################################
#                                                                               #
def _add_affiliations(author, affs):
    if affs:
        try:
            author['affiliation'].extend(affs)
        except KeyError:
            author['affiliation'] = affs
    return affs


def add_referenced_affiliation(author, affiliations):
    affs = [affiliations[ref] for ref in author.get("cross_ref", [])
            if ref in affiliations]
    return _add_affiliations(author, affs)


def add_group_affiliation(author, xml_author):
    affs = [get_value_in_tag(aff, "ce:textfn") for aff
            in xml_author.parentNode.getElementsByTagName('ce:affiliation')]
    return _add_affiliations(author, affs)


def add_global_affiliation(author, xml_author):
    def get_direct_cildren(element, tagname):
        affs = []
        for child in element.childNodes:
            try:
                if child.tagName == tagname:
                    affs.append(child)
            except AttributeError:
                pass
        return affs

    affs = []
    # get author group of author, but this is already done in group_affiliation
    parent = xml_author.parentNode
    while True:
        try:
            parent = parent.parentNode
            affs.extend([get_value_in_tag(aff, "ce:textfn") for aff
                         in get_direct_cildren(parent, 'ce:affiliation')])
        except AttributeError:
            break
    return _add_affiliations(author, affs)


def add_affiliations(authors, xml_authors, affiliations):
    for xml_author, author in zip(xml_authors, authors):
        if not add_referenced_affiliation(author, affiliations):
            add_group_affiliation(author, xml_author)
        add_global_affiliation(author, xml_author)


def find_affiliations(xml_doc):
    tmp = {}
    for aff in xml_doc.getElementsByTagName("ce:affiliation"):
        aff_id = aff.getAttribute("id").encode('utf-8')
        try:
            tmp[aff_id] = _affiliation_from_sa_field(aff)
        except:
            tmp[aff_id] = re.sub(r'^(\d+\ ?)', "",
                                 get_value_in_tag(aff, "ce:textfn"))
    return tmp
#                                                                               #
#################################################################################


def author_dic_from_xml(author):
    return {key: val for key, val in {
        'surname': get_value_in_tag(author, "ce:surname"),
        'given_name': get_value_in_tag(author, "ce:given-name"),
        'initials': get_value_in_tag(author, "ce:initials"),
        'orcid': unicode(author.getAttribute('orcid')),
        'email': next((xml_to_text(email)
                       for email in author.getElementsByTagName("ce:e-address")
                       if unicode(email.getAttribute("type")) in ('email', '')),
                      None),
        'cross_ref': [unicode(cross_ref.getAttribute("refid")) for cross_ref
                      in author.getElementsByTagName("ce:cross-ref")]
    }.items() if val is not None}


def get_authors(xml_doc):
        xml_authors = xml_doc.getElementsByTagName("ce:author")
        authors = [author_dic_from_xml(author) for author in xml_authors]
        add_affiliations(authors, xml_authors, find_affiliations(xml_doc))
        return authors


def check_record(record):
    first_author = True
    doc_ids = get_doc_ids(record['recid'])
    for doc_id in doc_ids:
        latest_file = get_latest_file(doc_id)
        try:
            xml = parse(latest_file)
        except Exception:
            #TODO: Warn
            continue
        authors = record['authors']
        authors_d = get_authors(xml)
        for author, author_d in zip(authors, authors_d):

            # clear author
            while author:
                author.popitem()

            # full_name
            author_name = (author_d['surname'], author_d.get(
                'given_name') or author_d.get('initials'))
            author_name = ('a', '%s, %s' % author_name)
            author['full_name'] = author_name

            # orcid, affiliation, email
            for key in ('orcid', 'affiliation', 'email'):
                try:
                    author[key] = author_d[key]
                except KeyError:
                    pass

            add_nations_field(subfields)  # TODO
