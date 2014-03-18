=================================
bqlib - BigQuery Python Library
=================================
BigQuery Python Library

See: `Google BigQuery <https://developers.google.com/bigquery/>`_

==============
Requirements
==============
* Python 2.6 or later (not support for 3.x)

========
Setup
========
::

    $ pip install bqlib

=============
How to use
=============

Single Query - BQJob
---------------------
| BQJob is a class for starting the BigQuery job and fetching the result.
| You can use either run\_sync(synchronous) or run\_async(asynchronous) method.

::

    from bqlib import BQJob

    project_id = 'example_project'
    query = 'SELECT foo FROM bar'
    http = authorized_http

    bqjob = BQJob(http, project_id, query=query)

    # run synchronously
    job_result = bqjob.run_sync()

    # or run asynchronously
    bqjob.run_async()
    # ... do other things ...

    job_result = bqjob.get_result()

    print job_result # [{u'foo': 10}, {u'foo': 20}, ...]

Multiple Queries - BQJobGroup
--------------------------------
| BQJobGroup is a class for putting multiple BQJobs into an one group.  
| Each BQJob in that group are executed concurrently.

::

    from bqlib import BQJob, BQJobGroup

    bqjob1 = BQJob(http, project_id, query=query)
    bqjob2 = BQJob(http, project_id, query=query)

    job_group = BQJobGroup([bqjob1, bqjob2])
    # synchronously
    results = job_group.run_sync()

    # or asynchronously
    job_group.run_async()
    # ... do other things ...

    results = job_group.get_results()

    print results # [[{'foo': 10}, {'foo': 20}], [{'bar': 'test'}]]

=====
Note
=====
* Concurrent Requests to BigQUery
    * Concurrent requests to BigQuery is restricted to 20 requests by `Quota Policy <https://developers.google.com/bigquery/docs/quota-policy>`_.
    * If you want to set up concurrent requests to 20, you also have to set up at traffic controls in `api-console <https://code.google.com/apis/console/>`_ page.

========
License
========
This library is disributed as MIT license.

========
History
========

2013-10-22 bqlib 0.0.1
-----------------------
* First release

2014-03-18 bqlib 0.0.2
-----------------------
* Bug fixes
