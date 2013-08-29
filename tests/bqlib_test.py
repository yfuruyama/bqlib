# -*- coding: utf-8 -*-
# Copyright 2013, Yuuki Furuyama
# Released under the MIT License.

"""bqlib tests"""

import os
import sys
from inspect import isclass
from contextlib import nested

import pytest
from mock import patch, Mock
from bqlib import BQJob, BQJobGroup, BQTable, BQHelper

### fixtures
_fixtures_convert_type = [
    ('STRING', 'hello', 'hello'),
    ('INTEGER', '1234', 1234),
    ('FLOAT', '0.123', 0.123),
    ('BOOLEAN', 'False', False),
    (u'STRING', u'hello', 'hello'),
    ('STRING', None, None),
    ('FOO', 'test', ValueError),
]

_fixtures_is_gae_run_time = [
    ('Google App Engine/2.7.1', True),
    ('Development/2.7', True),
    ('Heroku/2.7', False),
    (None, False),
]

_fixtures_build_fully_qualified_table_name = [
    ('4213455', 'bar', 'foo', '[4213455:bar.foo]'),
]

_fixtures_query_results = [
    ([{u'type': u'STRING', u'name': u'date', u'mode': u'NULLABLE'},
      {u'type': u'FLOAT', u'name': u'charge', u'mode': u'NULLABLE'}],
     [[u'2012-06-21', u'1.0'], [u'2012-06-23', u'2.0'], [u'2012-09-01', u'1000.0']],
     [{u'date': u'2012-06-21', u'charge': 1.0}, {u'date': u'2012-06-23', u'charge': 2.0}, {u'date': u'2012-09-01', u'charge': 1000.0},],
     ),
]


class BigqueryClientMock(object):
    """Mock for BigqueryClient class"""
    def __init__(self):
        self.wait_printer_factory = Mock()
    def setup_schema_and_rows(self, schema, rows):
        self._schema = {
            'fields': schema
        }
        self._rows = rows
    def Query(self, query, **kwargs):
        job = Mock()
        return job
    def ConstructObjectReference(self, job):
        return Mock()
    def WaitJob(self, job_reference, status='DONE',
              wait=sys.maxint, wait_printer_factory=None):
        job_fixture = {
          "kind": "bigquery#job",
          "configuration": {
            "query": {
              "destinationTable": {
              },
            },
          },
        }
        job = job_fixture
        return job
    def ReadSchemaAndRows(self, table_dict, max_rows):
        schema = self._schema
        rows = self._rows
        return (schema, rows)
    def GetTableSchema(self, table_dict):
        return self._schema
    def ReadTableRows(self, table_dict, max_rows):
        return self._rows


class BQJobFactory():
    def make_bqjob(self):
        return BQJob(Mock(), Mock(), bq_client=BigqueryClientMock(), 
                query=Mock(), verbose=False)


class BQJobGroupFactory():
    def make_bq_jobgroup(self, jobs=[]):
        return BQJobGroup(jobs=jobs)


class BQTableFactory():
    def make_bqtable(self):
        http = Mock()
        return BQTable(http, bq_client=BigqueryClientMock())


def pytest_funcarg__bqjob(request):
    return BQJobFactory().make_bqjob()


def pytest_funcarg__bq_jobgroup(request):
    bqjobs = [BQJobFactory().make_bqjob() for i in range(0, 2)]
    return BQJobGroupFactory().make_bq_jobgroup(jobs=bqjobs)


def pytest_funcarg__bqtable(request):
    return BQTableFactory().make_bqtable()


class TestBQJob(object):
    """test for BQJob class"""
    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    def test_run_async(self, bqjob):
        bqjob.run_async()
        assert bqjob.job_reference is not None

    @pytest.mark.parametrize(('schema', 'rows', 'expected'), _fixtures_query_results)
    def test_run_sync(self, bqjob, schema, rows, expected):
        bqjob.bq_client.setup_schema_and_rows(schema, rows)
        assert bqjob.run_sync() == expected

    @pytest.mark.parametrize(('schema', 'rows', 'expected'), _fixtures_query_results)
    def test_get_result(self, bqjob, schema, rows, expected):
        bqjob.bq_client.setup_schema_and_rows(schema, rows)
        assert bqjob.get_result() == expected


class TestBQJobGroup(object):
    """test for BQJobGroup class"""

    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    def test_get_jobs(self, bq_jobgroup):
        assert len(bq_jobgroup.get_jobs()) >= 0

    def test_add(self, bq_jobgroup, bqjob):
        orig_len = len(bq_jobgroup.get_jobs())
        bq_jobgroup.add(bqjob)
        assert len(bq_jobgroup.get_jobs()) == orig_len + 1

    def test_run_async(self, bq_jobgroup):
        bq_jobgroup.run_async()
        for bqjob in bq_jobgroup.get_jobs():
            assert bqjob.job_reference is not None

    @pytest.mark.parametrize(('schema', 'rows', 'results'), _fixtures_query_results)
    def test_get_results(self, bq_jobgroup, schema, rows, results):
        for bqjob in bq_jobgroup.get_jobs():
            bqjob.bq_client.setup_schema_and_rows(schema, rows)
        expected = [results for bqjob in range(0, len(bq_jobgroup.get_jobs()))]
        assert bq_jobgroup.get_results() == expected

    @pytest.mark.parametrize(('schema', 'rows', 'results'), _fixtures_query_results)
    def test_run_sync(self, bq_jobgroup, schema, rows, results):
        for bqjob in bq_jobgroup.get_jobs():
            bqjob.bq_client.setup_schema_and_rows(schema, rows)
        expected = [results for bqjob in range(0, len(bq_jobgroup.get_jobs()))]
        assert bq_jobgroup.run_sync() == expected


class TestBQTable(object):
    """test for BQTable class"""
    @pytest.mark.parametrize(('schema', 'rows', 'expected'), _fixtures_query_results)
    def test_read_rows(self, bqtable, schema, rows, expected):
        bqtable.bq_client.setup_schema_and_rows(schema, rows)
        assert bqtable.read_rows() == expected


class TestBQHelper(object):
    """test for BQHelper class"""

    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    @pytest.mark.parametrize(('runtime', 'expected'), _fixtures_is_gae_run_time)
    def test_is_gae_runtime(self, runtime, expected):
        if runtime:
            os.environ['SERVER_SOFTWARE'] = runtime
        assert BQHelper.is_gae_runtime() == expected

    @pytest.mark.parametrize(('field_type', 'value', 'expected'),
            _fixtures_convert_type)
    def test_convert_type(self, field_type, value, expected):
        with patch('bqlib.is_str_or_unicode', return_value=True):
            if isclass(expected) and issubclass(expected, Exception):
                with pytest.raises(expected):
                    BQHelper.convert_type(field_type, value)
            else:
                assert BQHelper.convert_type(field_type, value) == expected

    def test_retrieve_discovery_document(self):
        class _DiscoveryHTTPResponse(object):
            def read(self):
                return Mock()

        class _DiscoveryDocumentStorage(object):
            def __init__(self):
                self._storage = {}
            def get(self, key):
                self._storage.get(key)
            def set(self, key, value, *args, **kwargs):
                self._storage[key] = value

        with nested(
                patch('urllib2.urlopen', return_value=_DiscoveryHTTPResponse()),
                patch('bqlib.BQHelper.is_gae_runtime', return_value=False)
                ):
            storage = _DiscoveryDocumentStorage()
            # retrieve from HTTP request without storage
            assert BQHelper.retrieve_discovery_document() is not None
            # retrieve from HTTP request with storage
            assert BQHelper.retrieve_discovery_document(storage) is not None
            # retrieve from storage
            assert BQHelper.retrieve_discovery_document(storage) is not None

    @pytest.mark.parametrize(('project_id', 'dataset_id', 'table_id', 'expected'),
            _fixtures_build_fully_qualified_table_name)
    def test_build_fully_qualified_table_name(self, project_id, 
            dataset_id, table_id, expected):
        assert BQHelper.build_fully_qualified_table_name(
                project_id, dataset_id, table_id) == expected


class TestUtilityFunc(object):
    # TODO
    pass
