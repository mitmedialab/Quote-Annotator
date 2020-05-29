import logging
import sys

from quoteworker import get_db_client
from quoteworker.tasks import parse_quotes_to_db

logger = logging.getLogger(__name__)

# how many stories do you want to queue (set this to ~4 for testing, then a number more than your total to queue them all)
BATCH_SIZE = 200000

# we have timeouts with long stories, so split them if longer than this char max
MAX_CHAR_LEN = 10000

# the property of the Mongo document that will be used as the text to check for quotes
TEXT_PROP = 'story_text'

collection = get_db_client()

# how many left to do?
total = collection.count_documents({TEXT_PROP: {'$exists': True}})
unprocessed = collection.count_documents({'annotatedWithQuotes': {'$exists': False}, TEXT_PROP: {'$exists': True}})
logger.info("Stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))
#sys.exit()

# get ​stories with text without quotes from DB
logger.info("Fetching...")
queued = 0
chunk_count = 0

results = collection.find({'annotatedWithQuotes': {'$exists': False}, TEXT_PROP: {'$exists': True}}).limit(BATCH_SIZE)

for story in results:
    logger.debug("Story {}".format(story['stories_id']))
    if (TEXT_PROP in story) and (story[TEXT_PROP] is not None):  # only process if it has text
        # break longer stories into smaller chunks so we don't hit a CoreNLP timeout
        if len(story[TEXT_PROP]) > MAX_CHAR_LEN:
            chunks = [story[TEXT_PROP][i:i + MAX_CHAR_LEN] for i in range(0, len(story[TEXT_PROP]), MAX_CHAR_LEN)]
        else:
            chunks = [story[TEXT_PROP]]
        # queue up the job(s) to save extracted quote text to the DB
        for c in chunks:
            job = {'stories_id': story['stories_id'], 'text': c, 'add_to_quotes': True}
            parse_quotes_to_db.delay(job)
        # logger.info("  queueing up {} ({} chunks)".format(story['stories_id'], len(chunks)))
        chunk_count += len(chunks)
        queued += 1
    else:
        logger.warning("  Story {} has no text - skipping".format(story['stories_id']))
logger.info("Queued {} stories (in {} < {} char chunks)".format(queued, chunk_count, MAX_CHAR_LEN))
