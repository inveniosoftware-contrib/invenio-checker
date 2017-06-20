# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2013 CERN.
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

"""Change the title of records whose author is too young."""

from datetime import datetime

# Type must be specified
# Key may not start with 'arg_'
argument_schema = {
    'new_title': {'type': 'string', 'label': 'New Record Title'},
    # 'anumber': {'type': 'float'},
    # 'adt': {'type': 'datetime'},
    # 'atext': {'type': 'text'},
    'minimum_birthdate': {'type': 'date', 'label': 'Minimum birthdate of author'},
    # 'abool': {'type': 'boolean'},
    # 'adrop': {'type': 'choice', 'values': ['oNe', 'tWo']},
    # 'adrop': {'type': 'choice_multi', 'values': ['oNe', 'tWo']},
}


class CheckWhatever(object):

    # @staticmethod
    # def allowed_paths(cfg_args):
    #     # Must return list of strings
    #     return ''.join(cfg_args)

    # @staticmethod
    # def allowed_recids(cfg_args, requested_recids, all_recids):
    #     # Must return: set of recids
    #     return requested_recids

    # def setup_method(self, log):
    #     raise KeyError

    def check_fail(self, log, batch_recids, get_record, search, record, arguments):
        from uuid import uuid4
        record['foobar'] = 'baz ' + str(record['recid']) + str(uuid4())
        log('sup brah')
        return
        if record['control_number'] == 1:
            record['main_entry_personal_name']['personal_name'] = str(uuid4())
        if record['control_number'] in (1,2,3):
            log('whee')
        # if record['control_number'] in (4,5):
        #     raise KeyError('OMG WTF!?!?!')
        return
        # raise KeyError("OH NO")

        if 'title' not in record:
            log('Whoops. The author name was missing')
            raise

        if arguments['minimum_birthdate'] < datetime.now().date():
            log('Changing name for author ' + record['id'])
            record['title'] = arguments['new_name']
