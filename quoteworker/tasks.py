import requests
from celery.utils.log import get_task_logger
import json
import time

from quoteworker import NLP_URL, get_db_client
from quoteworker.celery import app

logger = get_task_logger(__name__)
SAVE_TO_DB = True  # really save results to the DB?
SNIPPET_WINDOW_SIZE = 100  # how many chars before or after the quote to save for context into the DB


def get_annotations(stories_id, text):
    r = requests.post(
        'http://' + NLP_URL + '/?properties={"annotators":"tokenize,ssplit,pos,lemma,ner,depparse,coref,quote","outputFormat":"json"}',
        data=text.encode('utf-8'))
    if r.status_code is not requests.codes.ok:
        if "java.lang.ArrayIndexOutOfBoundsException" in r.text:
            # tell Celery to retry it itself with a standard rate
            raise RuntimeError(
                "Failed on story {} - first one always fails, putting back on queue".format(stories_id))
        elif "We're sorry, but something went wrong (500)" in r.text:
            # save empty quotes, but processed
            logger.warning("Failed on story {} - some internal proxy error (prob too long)".format(stories_id))
            return {'quotes': []}
        else:
            # something else happened... maybe mismatch of quotes?
            raise RuntimeError("Failed on story {} - some other error: {}".format(stories_id, r.text[:100]))
    return r.json()


@app.task(serializer='json', bind=True)
def parse_quotes_to_db(self, job):
    """
    An asynchronous task that accepts a story with text, parses out any quotes, and saves info about them back to the DB
    :param self:
    :param story: a story object with 'text' and 'stories_id' properties
    """
    start = time.time()
    quotes = []
    if 'text' not in job:
        logger.error('{} - no text')
        return
    elif len(job['text']) == 0:
        logger.warning('{} - no chars in text')
        # OK to save the empty list of quotes here because we don't have any text in story
    else:
        try:
            document = get_annotations(job['stories_id'], job['text'])
        except json.JSONDecodeError:
            raise RuntimeError("Failed on story {} - returned OK but couldn't decode any json".format(job['stories_id']))
        # parse out quotes into a nice format
        logger.debug(document['quotes'])
        for q in document['quotes']:
            snippet_begin = max(0, q['beginIndex'] - SNIPPET_WINDOW_SIZE)
            snippet_end = min(q['endIndex'] + SNIPPET_WINDOW_SIZE, len(job['text']))
            info = {
                'index': q['id'],
                'text': q['text'],
                'begin_char': q['beginIndex'],
                'end_char': q['endIndex'],
                'begin_token': q['beginToken'],
                'end_token': q['endToken'],
                'begin_sentence': q['beginSentence'],
                'end_sentence': q['endSentence'],
                'snippet': job['text'][snippet_begin:snippet_end]
            }
            if 'mention' in q:
                info['mention'] = q['mention']
                info['mention_token_distance'] = q['tokenBegin'] - q['mentionBegin']
                info['mention_type'] = q['mentionType']
                info['mention_sieve'] = q['mentionSieve']
            if 'speaker' in q:
                info['speaker'] = q['speaker']
                info['canonicalSpeaker'] = q['canonicalSpeaker']
            quotes.append(info)
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the quotes to the DB
        # support adding to quotes that are already there (ie. for long stories that have been chunked into multiple jobs)
        if ('add_to_quotes' in job) and (job['add_to_quotes']):
            result = collection.update_one({'stories_id': int(job['stories_id'])},
                                           {
                                               '$set': {'annotatedWithQuotes': True},
                                               '$push': {'quotes': {'$each': quotes}},
                                           })
        else:
            result = collection.update_one({'stories_id': int(job['stories_id'])},
                                           {'$set': {'quotes': quotes, 'annotatedWithQuotes': True}})
        if result.modified_count == 0:
            logger.warning('{} - not modified ({} matching, {} modified'.format(
                job['stories_id'], result.matched_count, result.modified_count))
        else:
            end = time.time()
            logger.info('{}, {} quotes, {} chars, {} secs'.format(
                job['stories_id'], len(quotes), len(job['text']), end-start))
    else:
        logger.info('{} - {} quotes found (NOT SAVED)'.format(job['stories_id'], len(quotes)))
