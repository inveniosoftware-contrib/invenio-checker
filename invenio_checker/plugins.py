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

from .registry import plugin_files
from invenio.base.helpers import with_app_context


class Plugins(dict):
    def __init__(self, *args, **kwargs):
        super(Plugins, self).__init__(*args, **kwargs)

    @classmethod
    @with_app_context()
    def from_input(cls, user_choice):
        """Load plugins from a user-provided list of configuration files.

        :param user_choice: comma-separated list of requested plugins
            * may contain ALL
        :type  user_choice: str

        :returns: list of Plugins
        :rtype:   seq
        """
        user_plugins = set(user_choice.split(','))
        plugins = cls()
        if ALL in user_plugins:
            for filename, attrs in sorted(plugin_files.items()):
                plugins[filename] = attrs
            while ALL in user_plugins:
                user_plugins.remove(ALL)
        # TODO: Support extrnal files
        # for user_plugin in user_plugins:
        #     plugins.load_file(user_plugin)
        return plugins

    # def validate_module(self, module):
    #     pass

    # def load_file(self, filepath):
    #     from os.path import splitext, basename
    #     module_name = splitext(basename(filepath))[0]
    #     try:
    #         import importlib.machinery
    #         loader = importlib.machinery.SourceFileLoader(module_name, filepath)
    #         module = loader.load_module()
    #     except ImportError:
    #         try:
    #             import imp
    #             module = imp.load_source(module_name, filepath)
    #         except ImportError:
    #             pass
    #     # print self.validate_module(module)
    #     if module_name in self:
    #         raise DuplicateError(module_name)
    #     self[module_name] = module
