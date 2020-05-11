Media Cloud Story Quote Extractor
=================================

A helper that will extract quotes from a DB of stories from Media cloud. This starts with a Mongo 
database full of stories, where each document in the database is a `story` that has a `story_text`
property.

Requirements:
* Python3 - we use [pyenv](https://github.com/pyenv/pyenv) to manage different versions
p* Stanford CoreNLP Server - This requires you to be running a copy of the [Stanford CoreNLP Server](https://stanfordnlp.github.io/CoreNLP/corenlp-server.html),
([here is my fork](https://github.com/rahulbot/stanford-corenlp-docker) of the Docker install with some tweaks for the 
annotators we use for quote extraction).
* Redis - we use this via celery as a queue for parallel processing
* Mongo - this holds the story information  

Dev Installation
----------------

Install the dependencies `pip install -r requirements.txt`. 

Configuration
-------------

Copy the `.env.template` to `.env` and then edit it.

Use
---

Open up one terminal window and start the workers waiting: `celery worker -A quoteworker -l info`. Watch the log to see
if processing stories.

In another window start filling up the queue with `python queue-stories-from-db.py `.

Notes
-----

* To empty out your queue of jobs, run `redis-cli FLUSHALL`.
* Run a few quick sanity tests to make sure you are connected to the NLP server: `test.sh`
