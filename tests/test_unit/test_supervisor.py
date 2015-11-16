# -*- coding: utf-8 -*-
#
# This file is part of Invenio Checker.
# Copyright (C) 2015 CERN.
#
# Invenio Checker is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio Checker is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio Checker; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.


import pytest


class TestSupervisor(object):

    @pytest.mark.parametrize("_,recids,max_chunks,max_chunk_size,expected_chunk_cnt", [
        ("", set(), 10, 1000000, 0),
        ("", set(range(20)), 10, 1000000, 1),
        ("", set(range(2000)), 10, 1000000, 2),
        ("", set(range(10000)), 10, 1000000, 4),
        ("", set(range(100000)), 10, 1000000, 7),
        ("", set(range(1000000)), 10, 1000000, 10),
        ("", set(range(3000000)), 10, 1000000, 10),

        ("", set(range(500)), 10, 50, 10),
        ("", set(range(500)), 500, 5, 100),
    ])
    def test_chunk_recids(self, _, recids, max_chunks, max_chunk_size,
                          expected_chunk_cnt, app, mocker):

        mocker.patch('invenio_checker.config.current_app', app)

        from invenio_checker.clients.supervisor import chunk_recids
        result = chunk_recids(recids, max_chunks, max_chunk_size)
        lresult = list(result)

        # Did we cross the maximum allowed chunks?
        assert len(lresult) <= max_chunks
        assert len(lresult) == expected_chunk_cnt

        # Are there any chunks that are bigger than allowed?
        for chunk in lresult:
            assert len(chunk) <= max_chunk_size

        # Did we get all the records back out?
        from itertools import chain
        assert set(chain(*lresult)) == recids
