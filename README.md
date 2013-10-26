bqlib - BigQuery python library [![Build Status](https://travis-ci.org/addsict/bqlib.png)](https://travis-ci.org/addsict/bqlib)
------------------------------------------------
A BigQuery python library.  
This library is a wrapper for bigquery_client.py.  

Requirements
-------------
Python 2.6 or later (not support for 3.x)

Setup
-------

```sh
$ pip install bqlib
```

How to use
------------

### Single Query - BQJob
BQJob is a class to start the BigQuery job and fetch result.  
You can use either run\_sync(synchronous) or run\_async(asynchronous) method.

```python
from bqlib import BQJob

project_id = 'example_project'
query = 'SELECT foo FROM bar'
http = authorized_http

bqjob = BQJob(http,
              project_id, 
              query=query)

# run synchronously
job_result = bqjob.run_sync()

# or run asynchronously
bqjob.run_async()
# ... do other things ...
job_result = bqjob.get_result()

print job_result # [{u'foo': 10}, {u'foo': 20}, ...]
```

### Multiple Queries - BQJobGroup
BQJobGroup is a class for putting multiple BQJobs into an one group.  
Every BQJob in that group are executed concurrently.

```python
from bqlib import BQJob, BQJobGroup

bqjob1 = BQJob(http,
               project_id, 
               query=query)
bqjob2 = BQJob(http,
               project_id, 
               query=query)

job_group = BQJobGroup([bqjob1, bqjob2])
# synchronously
results = job_group.run_sync()
# or asynchronously
job_group.run_async()
results = job_group.get_results()

print results # [[{'foo': 10}, {'foo': 20}], [{'bar': 'test'}]]
```

How to test
----------
```sh
$ wget http://svn.zope.org/*checkout*/zc.buildout/trunk/bootstrap/bootstrap.py
$ pip install -U setuptools
$ python bootstrap.py -v 1.7.1
$ bin/buildout
$ bin/py.test -v tests/bqlib_test.py
```


Note
-----
- Concurrent Requests to BigQUery
    - Concurrent requests to BigQuery is restricted to 20 requests by [Quota Policy](https://developers.google.com/bigquery/docs/quota-policy).
    - If you want to set up concurrent requests to 20, you also have to set up at traffic controls in [api-console](https://code.google.com/apis/console/) page

License
-----------
This library is disributed as MIT license.

History
--------
- 2013-10-22 bqlib 0.0.1
    - First release
