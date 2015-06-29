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

from nose_parameterized import parameterized

from invenio.base.wrappers import lazy_import
from invenio.testsuite import InvenioTestCase

from common import is_exception_of

nations = lazy_import('invenio.modules.checker.checkerext.plugins.scoap3_nations')



class TestChanges(InvenioTestCase):
    """Test if desired changes occured."""

    @parameterized.expand((
        # test_name         , unstructured_affiliations                         , expected_countries
        ('one_affiliation'                 , [u'Institute of Nuclear Physics, Cracow, Poland']  , set(('Poland',))),
        ('multiple_affiliations'           , [u'National University of Singapore',
                                              u'The Marian Smoluchowski Institute of Physics, Jagiellonian University, Łojasiewicza 11, 30-348 Kraków, Poland',
                                              u'School of Physics and Material Science, Anhui University, 230039, Hefei, Anhui, People’s Republic of China']
                                                                                                , set(('Singapore', 'Poland', 'China'))),
        ('newline_and_dot_in_data'         , [u'National University of\nSingapore.']            , set(('Singapore',))),
        ('no_affiliation'                  , []                                                 , KeyError),
        ('no_country'                      , [u'INFN', u'Universita di Napoli']                 , KeyError),
        ('south_korea'                     , [u'Seoul National University of Korea']            , set(('South Korea',))),
        ('north_korea'                     , [u'Hamhŭng University of Education of North Korea']                                   , set(('North Korea',))),
        ('apostroph1'                      , [u'Hamhŭng University of Education of Democratic People’s Republic of Korea']         , set(('North Korea',))),
        ('apostroph2'                      , [u'Hamhŭng University of Education of Democratic People\'s Republic of Korea']        , set(('North Korea',))),
        ('uae'                             , [u'United Arab Emirates']                          , set(('United Arab Emirates',))),
        ('usa'                             , [u'United States of America']                      , set(('USA',))),
        ('two_usas'                        , [u'United States of America', u'United States of America']                            , set(('USA',))),
        ('usa_uae'                         , [u'United States of America and United Arab Emirates'] , KeyError),
        ('two_in_one'                      , [u'Greece and Deutschland'] , KeyError),
        ('cern_and_ch'                     , ['European Organization for Nuclear Research (CERN), Geneva, Switzerland'], set(('CERN',))),
    ))
    def test_extraction_of_countries_from_affilation(self, _, unstructured_affiliations, expected_countries):
        extract_countries = nations.extract_countries
        if is_exception_of(expected_countries, Exception):
            self.assertRaises(expected_countries,
                              extract_countries, unstructured_affiliations)
        else:
            countries = extract_countries(unstructured_affiliations)
            self.assertEquals(countries, expected_countries)

    @parameterized.expand((
        # test_name                  , record, expected_affiliations_per_author
        ('no_authors'                , {}, KeyError),
        ('no_author'                 , {'authors': {}}, KeyError),
        ('author_without_unstr_aff'  , {'authors': [{'first_name': 'John', 'last_name': 'Sykes'}]}, KeyError),
        ('yes_author'                , {'authors': [{'unstructured_affiliations': [u'University of Mongolia', u'University of Sudan']}]}, [['Mongolia', 'Sudan']]),
        ('multiple_authors'          , {'authors': [{'unstructured_affiliations': [u'University of Mongolia']},
                                                                   {'unstructured_affiliations': [u'University of Taiwan']}]},
                                                                  [['Mongolia'], ['Taiwan']]),
    ))
    def test_setting_of_countries_of_affilation_to_author(self, _, record, expected_result):
        check_record = nations.check_record
        if is_exception_of(expected_result, KeyError):
            self.assertRaises(expected_result,
                              check_record, record)
        else:
            resulting_record = check_record(record)
            for idx_author, author in enumerate(resulting_record['authors']):
                expected_author_affiliations = expected_result[idx_author]
                self.assertItemsEqual(author['affiliations'], expected_author_affiliations)
