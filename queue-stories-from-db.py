import logging
import sys

from quoteworker import get_db_client
from quoteworker.tasks import parse_quotes_to_db

logger = logging.getLogger(__name__)

BATCH_SIZE = 5000  # how many stories do you want to queue, at most
MAX_CHAR_LEN = 80000  # we have timeouts with long stories, so split them if longer than this char max
TEXT_PROP = 'story_text'

collection = get_db_client()


# how many left to do?
total = collection.count_documents({TEXT_PROP: {'$exists': True}})
unprocessed = collection.count_documents({'annotatedWithQuotes': {'$exists': False}, TEXT_PROP: {'$exists': True}})
logger.info("Stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))
# sys.exit()

# get ​stories with text without quotes from DB
logger.info("Fetching...")
queued = 0
chunk_count = 0
for story in collection.find({'annotatedWithQuotes': {'$exists': False}, TEXT_PROP: {'$exists': True}}).limit(BATCH_SIZE):
    logger.debug("Story {}".format(story['stories_id']))
    if 'annotatedWithQuotes' not in story:  # double check it doesn't have quotes already
        if (TEXT_PROP in story) and (story[TEXT_PROP] is not None):  # only process if it has text
            # break longer stories into smaller chunks so we don't hit a CoreNLP timeout
            if len(story[TEXT_PROP]) > MAX_CHAR_LEN:
                chunks = [story[TEXT_PROP][i:i + MAX_CHAR_LEN] for i in range(0, len(story[TEXT_PROP]), MAX_CHAR_LEN)]
            else:
                chunks = [story[TEXT_PROP]]
            # queue up the job(s) to save extracted quote text to the DB
            for c in chunks:
                job = {'stories_id': story['stories_id'], 'text': c}
                parse_quotes_to_db.delay(job)
            # logger.info("  queueing up {} ({} chunks)".format(story['stories_id'], len(chunks)))
            chunk_count += len(chunks)
            queued += 1
        else:
            logger.warning("  Story {} has no text - skipping".format(story['stories_id']))
    else:
        logger.warning("False positive!")
logger.info("Queued {} stories (in {} < {} char chunks)".format(queued, chunk_count, MAX_CHAR_LEN))
