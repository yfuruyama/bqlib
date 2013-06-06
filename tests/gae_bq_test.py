# -*- coding: utf-8 -*-
# Copyright 2013, Yuuki Furuyama
# Released under the MIT License.

"""gae-bq tests"""

import os
import sys
import datetime
from inspect import isclass
import urllib2
from StringIO import StringIO
from contextlib import nested

import pytest
from mock import patch, Mock

# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from dev_appserver import fix_sys_path
fix_sys_path()

from google.appengine.api import memcache
from google.appengine.ext import testbed
from bigquery_client import BigqueryClient
from gae_bq import BQHelper

_fixtures_convert_type = [
    ('STRING', 'hello', 'hello'),
    ('INTEGER', '1234', 1234),
    ('FLOAT', '0.123', 0.123),
    ('BOOLEAN', 'False', False),
    (u'STRING', u'hello', 'hello'),
    ('STRING', None, None),
    ('FOO', 'test', ValueError),
]

# _fixtures_bigquery_schema = [
	# [{u'type': u'STRING', u'name': u'date', u'mode': u'NULLABLE'}, 
	# {u'type': u'FLOAT', u'name': u'charge', u'mode': u'NULLABLE'}],
# ]
# _fixtures_bigquery_data = [
	# ([{u'type': u'STRING', u'name': u'date', u'mode': u'NULLABLE'}, {u'type': u'FLOAT', u'name': u'charge', u'mode': u'NULLABLE'}],
	# [[u'2012-06-21', u'1.0'], [u'2012-06-23', u'2.0'], [u'2012-09-01', u'1000.0']],
	# {'date': })
# ]

class TestBQJob(object):
    pass

class TestBQHelper(object):

    def setup_method(self, method):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        pass

    def teardown_method(self, method):
        self.testbed.deactivate()

    @pytest.mark.parametrize(('field_type', 'value', 'expected'), _fixtures_convert_type)
    def test_convert_type(self, field_type, value, expected):
        with patch('gae_bq.is_str_or_unicode', return_value=True):
            if isclass(expected) and issubclass(expected, Exception):
                with pytest.raises(expected):
                    BQHelper.convert_type(field_type, value)
            else:
                assert BQHelper.convert_type(field_type, value) == expected

    def test_retrieve_discovery_document(self):
        class _DiscoveryHTTPResponse(object):
            def read(self):
                return '{}'

        with patch('urllib2.urlopen', return_value=_DiscoveryHTTPResponse()):
            # from http request
            assert type(BQHelper.retrieve_discovery_document()) == str
            # from memcache
            assert type(BQHelper.retrieve_discovery_document()) == str
