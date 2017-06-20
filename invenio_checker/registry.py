# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Registry for checker module."""

import os
from flask.ext.registry import (
    PkgResourcesDirDiscoveryRegistry as FlaskPkgResourcesDirDiscoveryRegistry,
    ModuleAutoDiscoveryRegistry,
    RegistryProxy,
    RegistryError,
)

from invenio_ext.registry import DictModuleAutoDiscoverySubRegistry
from invenio_utils.datastructures import LazyDict

def _valuegetter(module):
    plugin_name = module.__name__.split('.')[-1]
    if plugin_name == '__init__':
        return None
    return module


class CheckerPluginRegistry(DictModuleAutoDiscoverySubRegistry):
    def keygetter(self, key, orig_value, class_):
        return orig_value.__name__

    def valuegetter(self, module):
        return _valuegetter(module)


class CheckerReporterRegistry(CheckerPluginRegistry):
    def valuegetter(self, module):
        return _valuegetter(module)


class PkgResourcesDirDiscoveryRegistry(FlaskPkgResourcesDirDiscoveryRegistry):
    def to_pathdict(self, test):
        """Return LazyDict representation."""
        return LazyDict(lambda: dict((os.path.basename(f), f)
                                     for f in self if test(f)))


checkerext = RegistryProxy('checkerext',
                           ModuleAutoDiscoveryRegistry,
                           'checkerext')

plugin_files = RegistryProxy('checkerext.checks',
                             CheckerPluginRegistry,
                             'checks',
                             registry_namespace=checkerext)


reporters_files = RegistryProxy('checkerext.reporters',
                                CheckerReporterRegistry,
                                'reporters',
                                registry_namespace=checkerext)
