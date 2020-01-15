import logging

from quoteworker import get_db_client
from quoteworker.tasks import parse_quotes_to_db

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000

collection = get_db_client()

# how many left to do?
total = collection.count_documents({'text': {'$exists': True}})
unprocessed = collection.count_documents({'quotes': {'$exists': False}, 'text': {'$exists': True}})
logger.info("Stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))

# get ​stories with text without quotes from DB
logger.info("Fetching...")
queued = 0
for story in collection.find({'quotes': {'$exists': False}, 'text': {'$exists': True}}).limit(BATCH_SIZE):
    parse_quotes_to_db.delay({'text': story['text'], 'stories_id': story['stories_id']})
    queued += 1
logger.info("  queued {} stories".format(queued))
