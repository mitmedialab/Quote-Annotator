import logging
import sys

from quoteworker import get_db_client
from quoteworker.tasks import parse_quotes_to_db

logger = logging.getLogger(__name__)

BATCH_SIZE = 20000
MAX_CHAR_LEN = 90000
STORY_TEXT_PROPERTY_NAME = 'story_text'

collection = get_db_client()


# how many left to do?
total = collection.count_documents({STORY_TEXT_PROPERTY_NAME: {'$exists': True}})
#unprocessed = collection.count_documents({'quotes': {'$exists': False}, STORY_TEXT_PROPERTY_NAME: {'$exists': True}})
unprocessed = collection.count_documents({'annotatedWithQuotes': {'$exists': False}, STORY_TEXT_PROPERTY_NAME: {'$exists': True}})
logger.info("Stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))
#sys.exit()

# get ​stories with text without quotes from DB
logger.info("Fetching...")
queued = 0
chunk_count = 0
# for story in collection.find({'quotes': {'$exists': False}, STORY_TEXT_PROPERTY_NAME: {'$exists': True}}).limit(BATCH_SIZE):
for story in collection.find({'annotatedWithQuotes': {'$exists': False}, STORY_TEXT_PROPERTY_NAME: {'$exists': True}}).limit(BATCH_SIZE):
    if STORY_TEXT_PROPERTY_NAME in story:
        if len(story[STORY_TEXT_PROPERTY_NAME]) > MAX_CHAR_LEN:
            chunks = [story[STORY_TEXT_PROPERTY_NAME][i:i + MAX_CHAR_LEN] for i in range(0, len(story[STORY_TEXT_PROPERTY_NAME]), MAX_CHAR_LEN)]
        else:
            chunks = [story[STORY_TEXT_PROPERTY_NAME]]
        for c in chunks:
            job = {'stories_id': story['stories_id'], 'text': c}
            parse_quotes_to_db.delay(job)
        # logger.info("  queueing up {} ({} chunks)".format(story['stories_id'], len(chunks)))
        chunk_count += len(chunks)
        queued += 1
    else:
        logger.warning("  Story {} has no text - skipping".format(story['stories_id']))
logger.info("Queued {} stories (in {} < {} char chunks)".format(queued, chunk_count, MAX_CHAR_LEN))
