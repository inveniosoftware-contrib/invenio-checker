# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2015 CERN.
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

import re
from itertools import product

import string
from collections import namedtuple

from invenio.scoap3_utils import NATIONS_DEFAULT_MAP


Nation = namedtuple('Nation', ['alias', 'name'])

all_nations = set(
    Nation(
        unicode(alias.lower(), 'utf-8'),
        unicode(name, 'utf-8')
    )
    for alias, name in NATIONS_DEFAULT_MAP.iteritems())
nations_with_space = set(n for n in all_nations if ' ' in n.alias)
nations_without_space = all_nations ^ nations_with_space


def normalize_unstr_affil(str_):
    return (re.sub(r'\s+', ' ', str_).strip().lower()
            .replace(u'’', '\'').replace(u'’', '\''))


def extract_countries(unstruct_affils):
    countries_of_affiliations = []
    for unstruct_affil, norm_unstruct_affil in \
            zip(unstruct_affils, map(normalize_unstr_affil, unstruct_affils)):

        # Search for aliases that have whitespace in them
        nations1 = set()
        for nation in nations_with_space:
            if nation.alias in norm_unstruct_affil:
                nations1.add(nation)
        for short_nation, long_nation in product(nations1, nations1):
            if short_nation == long_nation:
                continue
            if re.findall('.{c}|{c}.'.format(c=short_nation.alias),
                          long_nation.alias):
                try:
                    nations1.remove(short_nation)
                except KeyError:
                    pass
        if len(nations1) > 1:
            raise KeyError("Found multiple countries in one affiliation!",
                           nations1)

        # Search for aliases that have no whitespace in them
        nations2 = set()
        for p in string.punctuation:
            norm_unstruct_affil = norm_unstruct_affil.replace(p, ' ')
        for affiliation_string in norm_unstruct_affil.split():
            for nation in nations_without_space:
                if nation.alias == affiliation_string:
                    nations2.add(nation)
        for long_nation, short_nation in product(nations1, nations2):
            for substring in short_nation.name.split():
                if substring in long_nation.name:
                    try:
                        nations2.remove(short_nation)
                    except KeyError:
                        pass

        country_names = {nation.name for nation in nations1 | nations2}
        if {'CERN', 'Switzerland'}.issubset(country_names):
            country_names ^= {'Switzerland'}
        if len(country_names) == 1:
            countries_of_affiliations.append(country_names.pop())
        elif len(country_names) > 1:
            raise KeyError("Found multiple countries in one affiliation!",
                           unstruct_affil, country_names)
        else:
            raise KeyError("No countries found in affiliation!",
                           unstruct_affil)

    if not countries_of_affiliations:
        # Perhaps the loop never ran
        raise KeyError("Author has no affiliations!")
    else:
        return set(countries_of_affiliations)


def check_record(record):
    authors = record['authors']
    if not authors:
        raise KeyError
    for author in authors:
        try:
            unstructured_affiliations = author['unstructured_affiliations']
        except KeyError as e:
            e.args += ("Author has no 'unstructured_affiliations' field!", )
            raise e
        else:
            affiliations = extract_countries(unstructured_affiliations)
            author['affiliations'] = list(affiliations)
    return record
