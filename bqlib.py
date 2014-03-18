# -*- coding: utf-8 -*-
# Copyright 2014, Yuuki Furuyama
# Released under the MIT License.

""" bqlib - BigQuery python library """

import logging
import urllib2
import time
import sys
import datetime
import os
import re

from bigquery_client import (BigqueryError,
                             BigqueryNotFoundError,
                             BigqueryClient)


_API = 'bigquery'
_API_VERSION = 'v2'
_DISCOVERY_URI = ('https://www.googleapis.com/discovery/v1/apis/'
                  '{api}/{apiVersion}/rest')


class BQError(Exception):
    def __init__(self, message, error):
        super(BQError, self).__init__()
        self.message = message
        self.error = error

    def __str__(self):
        return self.message


class BaseBQ(object):
    def __init__(self, http, project_id=None,
                 discovery_document_storage=None, bq_client=None):
        """Initialize BaseBQ.

        Base class for BQJob, BQJobGroup, and BQTable.
        Required keywords:
            http: oauth2-authorized HTTP object
        """
        self.http = http
        if bq_client is None:
            if (discovery_document_storage is None and
                    BQHelper.is_gae_runtime()):
                from google.appengine.api import memcache
                discovery_document_storage = memcache

            discovery_document = BQHelper.retrieve_discovery_document(
                discovery_document_storage)
            bq_client = BigqueryClient(
                api=_API, api_version=_API_VERSION,
                project_id=project_id, discovery_document=discovery_document,
                wait_printer_factory=BigqueryClient.QuietWaitPrinter)

            # BigqueryClient requires 'credentials' to build
            # apiclient instance. But using 'authorized http'
            # is more versatile than using 'credentials'.
            bq_client._apiclient = BQHelper.build_apiclient(
                discovery_document,
                http)
        self.bq_client = bq_client


class BQJob(BaseBQ):
    """BigQuery Job model

    You can use this model to run BigQuery job.
    """
    def __init__(self, http, project_id, discovery_document_storage=None,
                 bq_client=None, query=None, verbose=True, **kwargs):
        """Initialize BQJob.

        Required keywords:
            http: oauth2-authorized HTTP object
            project_id: target project
        """
        super(BQJob, self).__init__(
            http,
            project_id=project_id,
            discovery_document_storage=discovery_document_storage,
            bq_client=bq_client
            )

        self.verbose = verbose
        self.query = query
        self.job_reference = None

    def run_sync(self, timeout=sys.maxint, **kwargs):
        self.run_async(**kwargs)
        try:
            return self.get_result(timeout=timeout)
        except StopIteration:
            raise BQError(message='timeout', error=[])

    def run_async(self, **kwargs):
        try:
            job = run_func_with_backoff(
                self.bq_client.Query, self.query,
                sync=False, **kwargs)
        except BigqueryError as err:
            message = None
            error = None
            if hasattr(err, 'message'):
                message = err.message
            if hasattr(err, 'error'):
                error = err.error
            raise BQError(message=message, error=error)
        self.job_reference = self.bq_client.ConstructObjectReference(job)

    def get_result(self, timeout=sys.maxint):
        """ get response from BigQuery

        same signature with async urlfetch : rpc.get_result()
        """
        bq_client = self.bq_client
        job_reference = self.job_reference
        job = bq_client.WaitJob(
            job_reference,
            wait=timeout,
            wait_printer_factory=bq_client.wait_printer_factory)
        if job['status'].get('errorResult') is not None:
            raise BQError(
                message=job['status']['errorResult']['message'],
                error=None
                )
        if self.verbose:
            self._print_verbose(job)
        bqtable = BQTable(
            self.http,
            bq_client=bq_client,
            table_dict=job['configuration']['query']['destinationTable']
            )
        return bqtable.read_rows()

    def _print_verbose(self, job_dict):
        log_format = u"""
        u############### Bigquery Query Statistics ###############
         projectId           : {project_id}
         jobId               : {job_id}
         query               : {query}
         totalBytesProcessed : {total_bytes} Bytes
         cacheHit            : {cache_hit}
        ######################################################
        """
        logging.info(log_format.format(
            project_id=job_dict['jobReference']['projectId'],
            job_id=job_dict['jobReference']['jobId'],
            query=job_dict['configuration']['query']['query'],
            total_bytes=job_dict['statistics']['query']['totalBytesProcessed'],
            cache_hit=job_dict['statistics']['query'].get('cacheHit', 'False'))
        )


class BQJobGroup(object):
    """Group for BigQuery job

    You can use this model to put multiple BQJob
    into an one group.
    'run_sync' and 'ryn_async' method are executed concurrently.
    """
    def __init__(self, jobs=[]):
        """Initialize BQJobGroup.

        Required keywords:
            None
        """
        self.jobs = jobs

    def add(self, bqjob):
        self.jobs.append(bqjob)

    def remove(self, bqjob):
        # TODO
        pass

    def get_jobs(self):
        return self.jobs

    def run_sync(self, timeout=sys.maxint):
        # start job
        self.run_async()

        # get job result
        results = []
        for job in self.jobs:
            results.append(job.get_result(timeout=timeout))
        return results

    def run_async(self):
        for job in self.jobs:
            job.run_async()

    def get_results(self):
        results = []
        for job in self.jobs:
            results.append(job.get_result())
        return results


class BQTable(BaseBQ):
    def __init__(self, http, project_id=None, dataset_id=None, table_id=None,
                 table_dict=None, discovery_document_storage=None,
                 bq_client=None, **kwargs):
        """Initialize BQTable.

        Required keywords:
            http: oauth2-authorized HTTP object
        """
        super(BQTable, self).__init__(
            http,
            discovery_document_storage=discovery_document_storage,
            bq_client=bq_client
            )
        if table_dict is None:
            table_dict = {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id
                }
        self.table_dict = table_dict

    def get_info(self):
        """get table information"""
        fqtn = BQHelper.build_fully_qualified_table_name(
            table_dict=self.table_dict,
            with_bracket=False)
        table_reference = self.bq_client.GetTableReference(fqtn)
        return self.bq_client.GetObjectInfo(table_reference)

    def get_schema(self):
        return self.bq_client.GetTableSchema(self.table_dict).get('fields', [])

    def read_rows(self):
        """read rows from table
        """
        schema = self.get_schema()
        table_dict = self.table_dict.copy()
        rows = self.bq_client.ReadTableRows(
            table_dict
            )
        results = []
        for row in rows:
            result = {}
            for (field, value) in zip(schema, row):
                converted_value = BQHelper.convert_type(field['type'], value)
                result[field['name']] = converted_value
            results.append(result)
        return results


class BQHelper(object):
    """Static helper methods and classes not provided by bigquery library."""
    def __init__(self, *unused_args, **unused_kwargs):
        raise NotImplementedError('cannot instantiate this static class')

    @staticmethod
    def is_gae_runtime():
        """Whether this runtime is Google App Engine"""
        server_software = os.environ.get('SERVER_SOFTWARE')
        if server_software is not None:
            num = r'\d+'
            gae = r'Google App Engine/%s\.%s\.%s' % (num, num, num)
            gae_dev = r'Development/%s\.%s' % (num, num)
            return bool(re.match(gae, server_software) or
                        re.match(gae_dev, server_software))
        else:
            return False

    @staticmethod
    def retrieve_discovery_document(storage=None):
        u""" Retrieve discovery document for BigQuery

        At first, discovery document is to be fetched from memcache.
        If discovery document doesn't exist in memcache,
        publish HTTP request to get a document discovery and set it to memcache
        """
        if storage is not None and hasattr(storage, 'get'):
            document_key = 'discovery_document'
            discovery_document = storage.get(document_key)
            if discovery_document is not None:
                return discovery_document

        params = {'api': _API, 'apiVersion': _API_VERSION}
        request_url = _DISCOVERY_URI.format(**params)
        logging.info('fetch discovery document from %s' % request_url)

        try:
            response = urllib2.urlopen(request_url)
            body = response.read()
        except urllib2.HTTPError as err:
            raise err

        if storage is not None and hasattr(storage, 'set'):
            storage.set(document_key, body, time=86400)  # 86400 = 1 day

        return body

    @staticmethod
    def build_apiclient(discovery_document, http):
        from apiclient import discovery
        from bigquery_client import BigqueryModel, BigqueryHttp

        bigquery_model = BigqueryModel()
        return discovery.build_from_document(
            discovery_document, http=http,
            model=bigquery_model,
            requestBuilder=BigqueryHttp.Factory(bigquery_model))

    @staticmethod
    def convert_type(field_type, value):
        """convert type of 'value' to 'field_type'

        filedtype: STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP
        TODO:
            required to implement Nested and Repeated Data type
            https://developers.google.com/bigquery/docs/data
        """
        if field_type is None or value is None:
            return None

        if not is_str_or_unicode(field_type) or not is_str_or_unicode(value):
            raise TypeError('field_type and value must be unicode or str type')

        field_type = field_type.upper()
        supported_field_type = (
            'STRING',
            'INTEGER',
            'FLOAT',
            'BOOLEAN',
            'TIMESTAMP',
            )
        if not field_type in supported_field_type:
            raise ValueError

        if field_type == 'STRING':
            return str(value)
        elif field_type == 'INTEGER':
            return int(value)
        elif field_type == 'FLOAT':
            return float(value)
        elif field_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            else:
                if value.lower() == 'true':
                    return True
                else:
                    return False
        elif field_type == 'TIMESTAMP':
            return datetime.datetime.utcfromtimestamp(float(value))

    @staticmethod
    def build_fully_qualified_table_name(
            project_id=None, dataset_id=None, table_id=None, table_dict=None,
            with_bracket=True):
        """Build fully qualified table name"""
        if table_dict is not None:
            project_id = table_dict.get('projectId')
            dataset_id = table_dict.get('datasetId')
            table_id = table_dict.get('tableId')
        if with_bracket:
            return "[%s:%s.%s]" % (project_id, dataset_id, table_id)
        else:
            return "%s:%s.%s" % (project_id, dataset_id, table_id)


"""utility functions"""


def is_str_or_unicode(obj):
    return isinstance(obj, unicode) or isinstance(obj, str)


def run_func_with_backoff(func, *args, **kwargs):
    count = 0
    retry = kwargs.get('retry', 3)
    backoff = kwargs.get('backoff', 1)
    while count < retry:
        try:
            return func(*args, **kwargs)
        except BigqueryNotFoundError as err:
            raise err
        except BigqueryError as err:
            logging.info(err.message)
            logging.info('will retry after %d seconds' % backoff)
            time.sleep(backoff)
            count += 1
            backoff *= 2
    raise BQError(message='%d times retried but failed' % retry, error=None)
