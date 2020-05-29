"""
A quick script to help benchmark how fast (ie. slow) quote annotation is
"""
import logging
import time

from quoteworker import get_db_client
logger = logging.getLogger(__name__)

# the property of the Mongo document that will be used as the text to check for quotes
TEXT_PROP = 'story_text'

SlEEP_DELAY = 60

collection = get_db_client()


# how many left to do?
def db_unprocessed():
    return collection.count_documents({'annotatedWithQuotes': {'$exists': False}, TEXT_PROP: {'$exists': True}})


total = collection.count_documents({TEXT_PROP: {'$exists': True}})
unprocessed = db_unprocessed()
logger.info("Start stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))

more_to_do = True

start_time = time.time()
while more_to_do:
    time.sleep(SlEEP_DELAY)
    unprocessed = db_unprocessed()
    remaining_ratio = unprocessed / total
    current_time = time.time()
    logger.info("Check: {} elapsed, {:.0%} remaining ({}/{})".format(
        int(current_time - start_time),
        remaining_ratio,
        unprocessed,
        total
    ))
    more_to_do = (remaining_ratio > .01)
end_time = time.time()

logger.info("End stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))

logger.info("Total: {} secs".format(end_time - start_time))
