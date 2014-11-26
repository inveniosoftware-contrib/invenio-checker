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

"""Record ID handling for checker."""

from intbitset import intbitset

from .common import ALL


def ids_from_input(ids_input):
    """Return the list of IDs to check for from user-input.

    :param ids_input: Comma-separated list of requested record IDs.
        May contain, or be ALL.
    :type  ids_input: str

    :returns: intbitset of IDs or ALL
    :rtype:   seq

    :raises:  ValueError
    """
    user_list = ids_input.split(',')
    if ALL in user_list:
        return ALL
    # TODO: Remove tuple() on next intbitset release (which supports generators)
    return intbitset(tuple(int(i) for i in user_list), sanity_checks=True)
