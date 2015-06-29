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

"""Add publisher information."""


CFG_JOURNAL_TO_PUBLISHER_MAP = {
    'physics letters b'               : 'Elsevier',
    'nuclear physics b'               : 'Elsevier',
    'advances in high energy physics' : 'Hindawi Publishing Corporation',
    'chinese phys. c'                 : 'Institute of Physics Publishing/Chinese Academy of Sciences',
    'jcap'                            : 'Institute of Physics Publishing/SISSA',
    'new j. phys.'                    : 'Institute of Physics Publishing/Deutsche Physikalische Gesellschaft',
    'acta physica polonica b'         : 'Jagiellonian University',
    'ptep'                            : 'Oxford University Press/Physical Society of Japan',
    'epjc'                            : 'Springer/Società Italiana di Fisica',
    'jhep'                            : 'Springer/SISSA',
}


def _resolve_publisher(journal):
    journal = journal.lower()
    try:
        return CFG_JOURNAL_TO_PUBLISHER_MAP[journal]
    except KeyError:
        # TODO: Warn
        return None


def _set_publisher(publisher, record):
    if 'publication_info' not in record:
        record['publication_info'] = {}
    record['publication_info']['publisher'] = publisher
    return record


def check_record(record):
    try:
        publisher = record['publication_info']['publisher']
    except KeyError:
        publisher = _resolve_publisher(record['publication_info']['journal'])

    record = _set_publisher(publisher, record)
    return record
