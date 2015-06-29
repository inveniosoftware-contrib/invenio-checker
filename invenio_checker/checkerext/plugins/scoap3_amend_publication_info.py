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


def _resolve_year_from_creation_date(record):
    try:
        return record['creation_date'].year
    except KeyError:
        return None


def _cleanup_publication_info(publication_info):
    for key, value in publication_info.items():
        if value in ('', '-'):
            del publication_info[key]
    return publication_info


def check_record(record):
    try:
        publication_info = defaultdict(record['publication_info'])
    except KeyError:
        publication_info = defaultdict()

    year = _resolve_year_from_creation_date(record)
    if year:
        publication_info['year'] = year

    publication_info = _cleanup_publication_info(publication_info)

    record['publication_info'] = dict(publication_info)
    return record
