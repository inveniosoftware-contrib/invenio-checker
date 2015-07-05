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

""" Bibcheck plugin to check fields have one of a defined set of values """


class CheckWhatever(object):

    # @staticmethod
    # def allowed_paths(cfg_args):
    #     # Must return list of strings
    #     return ''.join(cfg_args)

    # @staticmethod
    # def allowed_recids(cfg_args, requested_recids, all_recids, perform_request_search):
    #     # Must return: set of recids
    #     return requested_recids

    # def setup_method(self, log):
    #     raise KeyError

    def check_fail(self,log,record):
        log('record ' + str(record))
        # import os
        # os.path.join(tuple(), 1)
