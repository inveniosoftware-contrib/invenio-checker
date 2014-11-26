# -*- coding: utf-8 -*-
#
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

"""Registry for checker module."""

import os

import inspect
from flask.ext.registry import (
    PkgResourcesDirDiscoveryRegistry as FlaskPkgResourcesDirDiscoveryRegistry,
    ModuleAutoDiscoveryRegistry,
    RegistryProxy,
    RegistryError,
)

from invenio.ext.registry import (
    DictModuleAutoDiscoverySubRegistry,
)
from invenio.utils.datastructures import LazyDict


class CheckerPluginRegistry(DictModuleAutoDiscoverySubRegistry):
    def keygetter(self, key, orig_value, class_):
        return orig_value.__name__

    def valuegetter(self, class_or_module):
        if inspect.ismodule(class_or_module):
            plugin_name = class_or_module.__name__.split('.')[-1]
            if plugin_name == '__init__':
                # Ignore __init__ modules.
                return None

            def check_attr(attr_name):
                try:
                    attr = getattr(class_or_module, attr_name)
                    if not callable(attr):
                        raise TypeError
                except (AttributeError, TypeError):
                    raise RegistryError(
                        "Checker plugin's {plugin_name} `{attr}` could not be"
                        "loaded.".format(plugin_name=plugin_name,
                                         attr=attr_name))
                else:
                    return plugin_name

            if check_attr('check_record'):
                return plugin_name
        else:
            return class_or_module


class PkgResourcesDirDiscoveryRegistry(FlaskPkgResourcesDirDiscoveryRegistry):
    def to_pathdict(self, test):
        """Return LazyDict representation."""
        return LazyDict(lambda: dict((os.path.basename(f), f)
                                     for f in self if test(f)))


checkerext = RegistryProxy('checkerext', ModuleAutoDiscoveryRegistry, 'checkerext')

config_files_proxy = RegistryProxy('checkerext.configuration',
                                   PkgResourcesDirDiscoveryRegistry,
                                   'configuration',
                                   registry_namespace=checkerext)
config_files = config_files_proxy.to_pathdict(lambda basename:
                                              basename.endswith('.yaml'))

schema_files_proxy = RegistryProxy('checker.schema',
                                   PkgResourcesDirDiscoveryRegistry,
                                   'schema')
schema_files = schema_files_proxy.to_pathdict(lambda basename:
                                              basename.endswith('.yaml'))

plugin_files = RegistryProxy('checkerext.plugins',
                             CheckerPluginRegistry,
                             'plugins',
                             registry_namespace=checkerext)