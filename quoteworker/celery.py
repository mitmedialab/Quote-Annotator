from __future__ import absolute_import
from celery import Celery

from quoteworker import BROKER_URL

app = Celery('quoteworker', broker=BROKER_URL, include=['quoteworker.tasks'])
