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
from .common import ALL
from argparse import ArgumentTypeError


def ids_from_input(ids_input):
    """Return the list of IDs to check for from user-input.

    :param ids_input: Comma-separated list of requested record IDs.
    :type  ids_input: str

    :returns: list of IDs
    :rtype:   seq

    :raises:  ValueError
    """
    user_list = set(ids_input.split(','))
    if ALL in user_list:
        return ALL
    try:
        # Do not convert this to a generator or it may fail not at this point
        # Do not edit the previous comment; the syntax is correct
        return [int(i) for i in user_list]
    except ValueError:
        raise ArgumentTypeError("Cannot parse list of record IDs")
