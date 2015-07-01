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

"""Correct missing arxiv: prefix."""
from invenio.legacy.bibauthorid.general_utils import is_arxiv_id


arxiv_prefix = 'arXiv:'


def check_record(record):
    """Correct missing arxiv: prefix."""
    try:
        primary_report_number = record['primary_report_number']
    except KeyError:
        pass
    else:
        if is_arxiv_id(primary_report_number):
            if not primary_report_number.startswith(arxiv_prefix):
                primary_report_number = arxiv_prefix + primary_report_number
                record['primary_report_number'] = primary_report_number
    return record
