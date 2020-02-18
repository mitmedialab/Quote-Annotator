Media Cloud Story Quote Extractor
=================================

A helper that will extract quotes from a DB of stories

Dev Installation
----------------

 1. `virtualenv venv` to create your virtualenv
 2. `source venv/bin/activate` - to activate your virtualenv
 3. `pip install -r requirements.txt` - to install the dependencies

Configuration
-------------

1. Copy the `.env.template` to `.env` and then edit it.
2. Change the DB_NAME and COLLECTION_NAME in `quoteworker/__init__.py`

Use
---

Open up one terminal window and start the workers waiting: `celery worker -A quoteworker -l info`. Watch the log to see
if processing stories.

In another window start filling up the queue with `python queue-stories-from-db.py `.

Notes
-----

* To empty out your queue of jobs, run `redis-cli FLUSHALL`.
* Run a few quick sanity tests to make sure you are connected to the NLP server: `test.sh`
