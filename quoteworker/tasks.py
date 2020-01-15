import requests
from celery.utils.log import get_task_logger
import json

from quoteworker import NLP_URL, get_db_client
from quoteworker.celery import app

logger = get_task_logger(__name__)
SAVE_TO_DB = True  # really save results to the DB?
SNIPPET_WINDOW_SIZE = 100  # how many chars before or after the quote to save for context into the DB
TIMEOUT = 2 * 60 * 1000  # two minutes

def get_annotations(text):
    """
    Send the story text to a 3rd party Stanford NER install to parse out quotes (and everything else)
    :param text: the text of the story
    :return: JSON results from Stanford NER extration, with a 'quotes' property
    """
    response = requests.post(
        'http://'+NLP_URL+'/?properties={"timeout":50000,"annotators":"tokenize,ssplit,pos,lemma,ner,depparse,coref,quote","outputFormat":"json"}',
        data=text.encode('utf-8'))
    try:
        return response.json()
    except json.JSONDecodeError as jse:
        logger.error("Couldn't decode json, got: {}".format(response.text))
        raise jse


@app.task(serializer='json', bind=True)
def parse_quotes_to_db(self, story):
    """
    An asynchronous task that accepts a story with text, parses out any quotes, and saves info about them back to the DB
    :param self:
    :param story: a story object with 'text' and 'stories_id' properties
    """
    quotes = []
    if 'text' not in story:
        logger.error('{} - no text')
        return
    elif len('text') == 0:
        logger.warning('{} - no chars in text')
        # OK to save the empty list of quotes here because we don't have any text in story
    else:
        try:
            document = get_annotations(story['text'])
        except json.JSONDecodeError:
            return
        # parse out quotes into a nice format
        logger.debug(document['quotes'])
        for q in document['quotes']:
            snippet_begin = max(0, q['beginIndex'] - SNIPPET_WINDOW_SIZE)
            snippet_end = min(q['endIndex'] + SNIPPET_WINDOW_SIZE, len(story['text']))
            info = {
                'index': q['id'],
                'text': q['text'],
                'begin_char': q['beginIndex'],
                'end_char': q['endIndex'],
                'begin_token': q['beginToken'],
                'end_token': q['endToken'],
                'begin_sentence': q['beginSentence'],
                'end_sentence': q['endSentence'],
                'snippet': story['text'][snippet_begin:snippet_end]
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
    # make a local connection to DB, this is in its own thread
    collection = get_db_client()
    # write all the quotes to the DB
    if SAVE_TO_DB:
        collection.update_one({'stories_id': story['stories_id']}, {'$set': {'quotes': quotes}})
        logger.info('{} - {} quotes found (saved to DB)'.format(story['stories_id'], len(quotes)))
    else:
        logger.info('{} - {} quotes found (NOT SAVED)'.format(story['stories_id'], len(quotes)))
